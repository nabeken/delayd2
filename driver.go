package delayd2

import (
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"net"
	"time"

	"github.com/lib/pq"
)

const orphanedWorkerID = "__ORPHANED__"

var (
	ErrMessageDuplicated = errors.New("driver: message is duplicated")
	ErrSessionRegistered = errors.New("driver: session is already registered")
)

type Driver interface {
	RegisterSession() error
	DeregisterSession() error
	KeepAliveSession() error
	Enqueue(string, int64, string, string) error
	ResetActive() (int64, error)
	MarkActive(time.Time) (int64, error)
	MarkOrphaned() error
	AdoptOrphans() (int64, error)
	RemoveMessages(...string) error
	GetActiveMessages() ([]*QueueMessage, error)
}

func init() {
	sql.Register("postgres-delayd2", &pqDrv{10 * time.Second})
}

type pqDrv struct {
	keepalive time.Duration
}

func (d *pqDrv) Open(name string) (driver.Conn, error) {
	return pq.DialOpen(&pqDialer{d.keepalive}, name)
}

type pqDialer struct {
	keepalive time.Duration
}

func (d *pqDialer) setKeepAlive(conn net.Conn) {
	if tcpConn, ok := conn.(*net.TCPConn); ok {
		tcpConn.SetKeepAlivePeriod(d.keepalive)
		tcpConn.SetKeepAlive(true)
	}
}

func (d *pqDialer) Dial(ntw, addr string) (net.Conn, error) {
	conn, err := net.Dial(ntw, addr)
	if err != nil {
		return nil, err
	}
	d.setKeepAlive(conn)
	return conn, nil
}

func (d *pqDialer) DialTimeout(ntw, addr string, timeout time.Duration) (net.Conn, error) {
	conn, err := net.DialTimeout(ntw, addr, timeout)
	if err != nil {
		return nil, err
	}
	d.setKeepAlive(conn)
	return conn, nil
}

type pqDriver struct {
	workerID string
	db       *sql.DB
}

func NewDriver(workerID string, db *sql.DB) Driver {
	return &pqDriver{
		workerID: workerID,
		db:       db,
	}
}

func (d *pqDriver) RegisterSession() error {
	keepAlivedAt := time.Now()
	_, err := d.db.Exec(`
		INSERT
		INTO
		  session
		VALUES
			($1, $2)
	;`, d.workerID, keepAlivedAt)

	if perr, ok := err.(*pq.Error); ok && perr.Code.Class() == "23" {
		return ErrSessionRegistered
	}
	return err
}

func (d *pqDriver) DeregisterSession() error {
	_, err := d.db.Exec(`
		DELETE
		FROM
		  session
		WHERE
		  worker_id = $1
	;`, d.workerID)
	return err
}

func (d *pqDriver) KeepAliveSession() error {
	keepAlivedAt := time.Now()
	_, err := d.db.Exec(`
		UPDATE session
		SET
		  keepalived_at = $1
		WHERE
		  worker_id = $2
	;`, keepAlivedAt, d.workerID)
	return err
}

func (d *pqDriver) Enqueue(queueId string, delay int64, relayTo string, payload string) error {
	releaseAt := time.Now().Add(time.Duration(delay) * time.Second)
	_, err := d.db.Exec(`
		INSERT
		INTO
		  queue
		VALUES
			($1, $2, $3, $4, $5)
	;`, queueId, d.workerID, releaseAt, relayTo, payload)

	if perr, ok := err.(*pq.Error); ok && perr.Code.Class() == "23" {
		return ErrMessageDuplicated
	}
	return err
}

func (d *pqDriver) ResetActive() (int64, error) {
	ret, err := d.db.Exec(`
		DELETE
		FROM
		  active
		WHERE
		  worker_id = $1
		;
	`, d.workerID)
	if err != nil {
		return 0, err
	}
	return ret.RowsAffected()
}

func (d *pqDriver) MarkOrphaned() error {
	_, err := d.db.Exec(`
		UPDATE queue
		SET
		  worker_id = $1
		WHERE
		  worker_id = $2
	;`, orphanedWorkerID, d.workerID)
	return err
}

func (d *pqDriver) AdoptOrphans() (int64, error) {
	ret, err := d.db.Exec(`
		UPDATE queue
		SET
		  worker_id = $1
		WHERE
		  worker_id = $2
	;`, d.workerID, orphanedWorkerID)
	if err != nil {
		return 0, err
	}
	return ret.RowsAffected()
}

func (d *pqDriver) MarkActive(now time.Time) (int64, error) {
	ret, err := d.db.Exec(`
		INSERT
		INTO
		  active
		SELECT queue_id, worker_id
		FROM
		  queue
		WHERE
		      worker_id = $1
		  AND release_at < $2
		  AND NOT EXISTS (
			  SELECT 1 FROM active WHERE queue_id = queue.queue_id
		  )
		ORDER BY queue_id
		LIMIT 10000

	;`, d.workerID, now)
	if err != nil {
		return 0, err
	}
	return ret.RowsAffected()
}

func (d *pqDriver) RemoveMessages(queueIDs ...string) error {
	ids := make([]interface{}, len(queueIDs))
	for i := range queueIDs {
		ids[i] = queueIDs[i]
	}

	_, err := d.db.Exec(`
		DELETE
		FROM
			queue
		WHERE
			queue_id IN `+BuildPlaceHolders(len(queueIDs))+`
	;`, ids...)
	return err
}

func (d *pqDriver) GetActiveMessages() ([]*QueueMessage, error) {
	rows, err := d.db.Query(`
		SELECT queue.queue_id, queue.worker_id, queue.release_at, queue.relay_to, queue.payload
		FROM
		  queue INNER JOIN active USING (queue_id)
		WHERE
		  queue.worker_id = $1
		ORDER BY queue.release_at
		;
	`, d.workerID)

	if err != nil {
		return nil, err
	}
	defer rows.Close()

	messages := []*QueueMessage{}
	for rows.Next() {
		var m QueueMessage
		if err := rows.Scan(&m.QueueID, &m.WorkerID, &m.ReleaseAt, &m.RelayTo, &m.Payload); err != nil {
			return nil, err
		}
		messages = append(messages, &m)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return messages, nil
}

func BuildPlaceHolders(n int) string {
	if n < 1 {
		return ""
	}
	ret := "($1"
	if n != 1 {
		for i := 2; i < n+1; i++ {
			ret += fmt.Sprintf(", $%d", i)
		}
	}
	return ret + ")"
}
