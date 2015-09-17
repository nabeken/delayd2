package delayd2

import (
	"database/sql"
	"errors"
	"time"

	"github.com/lib/pq"
)

var ErrMessageDuplicated = errors.New("driver: message is duplicated")

type Driver interface {
	Enqueue(string, int64, string, string) error
	ResetActive() (int64, error)
	MarkActive(time.Time) (int64, error)
	RemoveMessage(string) error
	GetActiveMessages() ([]*QueueMessage, error)
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

	;`, d.workerID, now)
	if err != nil {
		return 0, err
	}
	return ret.RowsAffected()
}

func (d *pqDriver) RemoveMessage(queueID string) error {
	_, err := d.db.Exec(`
		DELETE
		FROM
			queue
		WHERE
			queue_id = $1
	;`, queueID)
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
		LIMIT 1000
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
