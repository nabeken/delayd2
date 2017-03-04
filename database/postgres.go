package database

import (
	"database/sql"
	"database/sql/driver"
	"net"
	"time"

	"github.com/lib/pq"
)

// pqDriver is the wrapper for lib/pq to set TCP Keep-Alive.
type pqDriver struct {
	keepalive time.Duration
}

func (d *pqDriver) Open(name string) (driver.Conn, error) {
	return pq.DialOpen(&postgresDialer{d.keepalive}, name)
}

type postgresDialer struct {
	keepalive time.Duration
}

func (d *postgresDialer) setKeepAlive(conn net.Conn) {
	if tcpConn, ok := conn.(*net.TCPConn); ok {
		tcpConn.SetKeepAlivePeriod(d.keepalive)
		tcpConn.SetKeepAlive(true)
	}
}

func (d *postgresDialer) Dial(ntw, addr string) (net.Conn, error) {
	conn, err := net.Dial(ntw, addr)
	if err != nil {
		return nil, err
	}
	d.setKeepAlive(conn)
	return conn, nil
}

func (d *postgresDialer) DialTimeout(ntw, addr string, timeout time.Duration) (net.Conn, error) {
	conn, err := net.DialTimeout(ntw, addr, timeout)
	if err != nil {
		return nil, err
	}
	d.setKeepAlive(conn)
	return conn, nil
}

func NewDriver(workerID string, db *sql.DB) Driver {
	return &postgresDriver{
		workerID: workerID,
		db:       db,
	}
}

type postgresDriver struct {
	workerID string
	db       *sql.DB
}

func (d *postgresDriver) RegisterSession() error {
	keepAlivedAt := time.Now()
	_, err := d.db.Exec(`
		INSERT
		INTO
		  session
		VALUES
			($1, $2)
	;`, d.workerID, keepAlivedAt)

	return pqErrorOrElse(err)
}

func (d *postgresDriver) DeregisterSession() error {
	_, err := d.db.Exec(`
		DELETE
		FROM
		  session
		WHERE
		  worker_id = $1
	;`, d.workerID)
	return pqErrorOrElse(err)
}

func (d *postgresDriver) KeepAliveSession() error {
	keepAlivedAt := time.Now()
	_, err := d.db.Exec(`
		UPDATE session
		SET
		  keepalived_at = $1
		WHERE
		  worker_id = $2
	;`, keepAlivedAt, d.workerID)
	return pqErrorOrElse(err)
}

func (d *postgresDriver) Enqueue(queueId string, delay int64, relayTo string, payload string) error {
	releaseAt := time.Now().Add(time.Duration(delay) * time.Second)
	_, err := d.db.Exec(`
		INSERT
		INTO
		  queue
		VALUES
			($1, $2, $3, $4, $5)
	;`, queueId, d.workerID, releaseAt, relayTo, payload)

	return pqErrorOrElse(err)
}

func (d *postgresDriver) ResetActive() (int64, error) {
	ret, err := d.db.Exec(`
		DELETE
		FROM
		  active
		WHERE
		  worker_id = $1
		;
	`, d.workerID)
	if err != nil {
		return 0, pqErrorOrElse(err)
	}
	return ret.RowsAffected()
}

func (d *postgresDriver) MarkActive(now time.Time) (int64, error) {
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
	;`, d.workerID, now)
	if err != nil {
		return 0, pqErrorOrElse(err)
	}
	return ret.RowsAffected()
}

func (d *postgresDriver) RemoveMessages(queueIDs ...string) error {
	ids := make([]interface{}, len(queueIDs))
	for i := range queueIDs {
		ids[i] = queueIDs[i]
	}

	_, err := d.db.Exec(`
		DELETE
		FROM
			queue
		WHERE
			queue_id = ANY($1)
	;`, pq.Array(ids))
	return err
}

func (d *postgresDriver) GetActiveMessages() ([]*Message, error) {
	rows, err := d.db.Query(`
		SELECT queue.queue_id, queue.worker_id, queue.release_at, queue.relay_to, queue.payload
		FROM
		  queue INNER JOIN active USING (queue_id)
		WHERE
		  queue.worker_id = $1
	;`, d.workerID)

	if err != nil {
		return nil, pqErrorOrElse(err)
	}
	defer rows.Close()

	messages := []*Message{}
	for rows.Next() {
		var m Message
		if err := rows.Scan(&m.ID, &m.WorkerID, &m.ReleaseAt, &m.RelayTo, &m.Payload); err != nil {
			return nil, pqErrorOrElse(err)
		}
		messages = append(messages, &m)
	}
	if err := rows.Err(); err != nil {
		return nil, pqErrorOrElse(err)
	}
	return messages, nil
}

func pqErrorOrElse(err error) error {
	if err != nil {
		if perr, ok := err.(*pq.Error); ok {
			return &pqError{perr}
		}
	}
	return err
}

var _ Driver = (*postgresDriver)(nil)

func init() {
	sql.Register("postgres-delayd2", &pqDriver{10 * time.Second})
}
