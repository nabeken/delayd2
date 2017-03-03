package database

import (
	"database/sql"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestDriverSession(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping the database tests in short mode.")
	}

	require := require.New(t)
	drv := &postgresDriver{
		workerID: "testing-1",
		db:       newTestDriver(),
	}
	defer drv.db.Exec("DELETE FROM session;")

	{
		// No error even if there is no session
		require.NoError(drv.DeregisterSession())
	}

	{
		require.NoError(drv.RegisterSession())
		err := drv.RegisterSession()
		require.Error(err)
		require.True(IsConflictError(err))

		// deregister and re-register
		require.NoError(drv.DeregisterSession())
		require.NoError(drv.RegisterSession())
	}

	{
		var initializedAt time.Time
		err := drv.db.QueryRow(
			"SELECT keepalived_at FROM session WHERE worker_id = $1", drv.workerID).
			Scan(&initializedAt)

		require.NoError(err)

		var now time.Time
		require.NoError(drv.KeepAliveSession())
		err = drv.db.QueryRow(
			"SELECT keepalived_at FROM session WHERE worker_id = $1", drv.workerID).
			Scan(&now)

		require.True(now.After(initializedAt))
	}
}

func TestDriver(t *testing.T) {
	require := require.New(t)

	drv1 := &postgresDriver{
		workerID: "testing-1",
		db:       newTestDriver(),
	}
	drv2 := &postgresDriver{
		workerID: "testing-2",
		db:       newTestDriver(),
	}
	defer drv1.db.Exec("DELETE FROM queue;")

	testCases := map[string]struct {
		Messages []*Message
		Driver   Driver
	}{
		"testing-1": {
			Driver: drv1,
			Messages: []*Message{
				{
					ID:       "queue-1",
					WorkerID: "testing-1",
					RelayTo:  "relay-to",
					Payload:  "payload-1",
				},
				{
					ID:       "queue-2",
					WorkerID: "testing-1",
					RelayTo:  "relay-to",
					Payload:  "payload-2",
				},
			},
		},
		"testing-2": {
			Driver: drv2,
			Messages: []*Message{
				{
					ID:       "queue-3",
					WorkerID: "testing-2",
					RelayTo:  "relay-to",
					Payload:  "payload-3",
				},
				{
					ID:       "queue-4",
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
			require.NoError(drv.Enqueue(m.ID, 60, m.RelayTo, m.Payload))
		}

		n, err := drv.MarkActive(now.Add(3 * time.Minute))
		require.NoError(err)
		require.Equal(int64(2), n)

		n, err = drv.MarkActive(now.Add(3 * time.Minute))
		require.NoError(err)
		require.Equal(int64(0), n)

		n, err = drv.ResetActive()
		require.NoError(err)
		require.Equal(int64(2), n)

		n, err = drv.MarkActive(now.Add(3 * time.Minute))
		require.NoError(err)
		require.Equal(int64(2), n)

		messages, err := drv.GetActiveMessages()
		require.NoError(err)
		require.Len(messages, 2)

		expectedReleaseAt := now.Add(59 * time.Second)
		var ids []string
		for i := range messages {
			// tweak to ignore ReleaseAt
			tc.Messages[i].ReleaseAt = messages[i].ReleaseAt
			require.Equal(tc.Messages[i], messages[i])
			require.True(
				messages[i].ReleaseAt.After(expectedReleaseAt),
				"release_at must be after 60 seconds: expected %s, got %s",
				tc.Messages[i].ReleaseAt, messages[i].ReleaseAt,
			)
			ids = append(ids, messages[i].ID)
		}

		require.NoError(drv.RemoveMessages(ids...))

		n, err = drv.MarkActive(now.Add(1 * time.Minute))
		require.NoError(err)
		require.Equal(int64(0), n)
	}
}

func newTestDriver() *sql.DB {
	db, err := sql.Open("postgres", "dbname=delayd2-test sslmode=disable")
	if err != nil {
		panic(err.Error())
	}
	return db
}
