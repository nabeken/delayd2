package delayd2

import (
	"database/sql"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestDriver(t *testing.T) {
	assert := assert.New(t)

	drv1 := &pqDriver{
		workerID: "testing-1",
		db:       newDriver(),
	}
	drv2 := &pqDriver{
		workerID: "testing-2",
		db:       newDriver(),
	}
	defer drv1.db.Exec("DELETE FROM queue;")

	testCases := map[string]struct {
		Messages []*QueueMessage
		Driver   Driver
	}{
		"testing-1": {
			Driver: drv1,
			Messages: []*QueueMessage{
				{
					QueueID:  "queue-1",
					WorkerID: "testing-1",
					RelayTo:  "relay-to",
					Payload:  "payload-1",
				},
				{
					QueueID:  "queue-2",
					WorkerID: "testing-1",
					RelayTo:  "relay-to",
					Payload:  "payload-2",
				},
			},
		},
		"testing-2": {
			Driver: drv2,
			Messages: []*QueueMessage{
				{
					QueueID:  "queue-3",
					WorkerID: "testing-2",
					RelayTo:  "relay-to",
					Payload:  "payload-3",
				},
				{
					QueueID:  "queue-4",
					WorkerID: "testing-2",
					RelayTo:  "relay-to",
					Payload:  "payload-4",
				},
			},
		},
	}

	for _, tc := range testCases {
		now := time.Now()
		drv := tc.Driver
		for _, m := range tc.Messages {
			assert.NoError(drv.Enqueue(m.QueueID, 60, m.RelayTo, m.Payload))
		}

		n, err := drv.MarkActive(now.Add(3 * time.Minute))
		assert.NoError(err)
		assert.Equal(int64(2), n)

		n, err = drv.ResetActive()
		assert.NoError(err)
		assert.Equal(int64(2), n)

		n, err = drv.MarkActive(now.Add(3 * time.Minute))
		assert.NoError(err)
		assert.Equal(int64(2), n)

		messages, err := drv.GetActiveMessages()
		assert.NoError(err)
		assert.Len(messages, 2)

		expectedReleaseAt := now.Add(59 * time.Second)
		for i := range messages {
			// tweak to ignore ReleaseAt
			tc.Messages[i].ReleaseAt = messages[i].ReleaseAt
			assert.Equal(tc.Messages[i], messages[i])
			assert.True(
				messages[i].ReleaseAt.After(expectedReleaseAt),
				"release_at must be after 60 seconds: expected %s, got %s",
				tc.Messages[i].ReleaseAt, messages[i].ReleaseAt,
			)

			assert.NoError(drv.RemoveMessage(messages[i].QueueID))
		}

		n, err = drv.MarkActive(now.Add(1 * time.Minute))
		assert.NoError(err)
		assert.Equal(int64(0), n)
	}
}

func newDriver() *sql.DB {
	db, err := sql.Open("postgres", "dbname=delayd2-test sslmode=disable")
	if err != nil {
		panic(err.Error())
	}
	return db
}
