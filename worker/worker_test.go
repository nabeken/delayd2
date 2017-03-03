package worker

import (
	"fmt"
	"testing"

	"github.com/nabeken/delayd2/database"
	"github.com/stretchr/testify/require"
)

func TestBuildBatchMap(t *testing.T) {
	require := require.New(t)

	messages := []*database.Message{}
	for i := 0; i < 25; i++ {
		messages = append(messages, &database.Message{
			ID:      fmt.Sprintf("queue-%d", i),
			RelayTo: "target-1",
			Payload: fmt.Sprintf("payload-%d", i),
		})
	}

	messages = append(messages, []*database.Message{
		{
			ID:      "queue-26",
			RelayTo: "target-2",
			Payload: "payload-26",
		},
		{
			ID:      "queue-27",
			RelayTo: "target-2",
			Payload: "payload-27",
		},
		{
			ID:      "queue-28",
			RelayTo: "target-3",
			Payload: "payload-28",
		},
	}...)

	actual := BuildBatchMap(messages)
	for _, expect := range []struct {
		RelayTo       string
		MessagesLen   []int
		FirstPayloads []string
	}{
		{
			RelayTo:       "target-1",
			MessagesLen:   []int{10, 10, 5},
			FirstPayloads: []string{"payload-0", "payload-10", "payload-20"},
		},
		{
			RelayTo:       "target-2",
			MessagesLen:   []int{2},
			FirstPayloads: []string{"payload-26"},
		},
		{
			RelayTo:       "target-3",
			MessagesLen:   []int{1},
			FirstPayloads: []string{"payload-28"},
		},
	} {
		require.Len(actual[expect.RelayTo], len(expect.MessagesLen))

		for i := range actual[expect.RelayTo] {
			require.Len(actual[expect.RelayTo][i], expect.MessagesLen[i])
			require.Equal(expect.FirstPayloads[i], actual[expect.RelayTo][i][0].Payload)
		}
	}
}
