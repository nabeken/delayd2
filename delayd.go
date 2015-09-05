// delayd2 is an available setTimeout() service for scheduling message sends
package delayd2

import (
	"database/sql"
	"time"
)

type QueueMessage struct {
	ID        string
	WorkerID  string
	ReleaseAt time.Time
	RelayTo   string
	Payload   []byte
}

type Delayd struct {
	workerID string
	db       *sql.DB
}

func New(workerID string, db *sql.DB) *Delayd {
	return &Delayd{
		workerID: workerID,
		db:       db,
	}
}

func (d *Delayd) ResetActive() (int64, error) {
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

func (d *Delayd) MarkActive(now time.Time) (int64, error) {
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
	;`, d.workerID, now)
	if err != nil {
		return 0, err
	}
	return ret.RowsAffected()
}

func (d *Delayd) RemoveMessage(queueID string) error {
	_, err := d.db.Exec(`
		DELETE
		FROM
			queue
		WHERE
			queue_id = $1
	;`, queueID)
	return err
}

func (d *Delayd) GetActiveMessages() ([]*QueueMessage, error) {
	rows, err := d.db.Query(`
		SELECT queue.queue_id, queue.worker_id, queue.release_at, queue.relay_to, queue.payload
		FROM
		  queue INNER JOIN active USING (queue_id)
		WHERE
		  queue.worker_id = $1
		;
	`, d.workerID)

	if err != nil {
		return nil, err
	}
	defer rows.Close()

	messages := []*QueueMessage{}
	for rows.Next() {
		var m QueueMessage
		if err := rows.Scan(&m.ID, &m.WorkerID, &m.ReleaseAt, &m.RelayTo, &m.Payload); err != nil {
			return nil, err
		}
		messages = append(messages, &m)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return messages, nil
}
