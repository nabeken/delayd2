package delayd2

import (
	"flag"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/nabeken/aws-go-sqs/queue"
	"github.com/stretchr/testify/assert"
)

var doInteg = flag.Bool("integ", false, "enable integration tests")

func TestWorker(t *testing.T) {
	assert := assert.New(t)

	if !*doInteg {
		t.Skip("skipping integration testing")
	}

	queueName := os.Getenv("TEST_SQS_QUEUE_NAME")
	if queueName == "" {
		t.Fatal("TEST_SQS_QUEUE_NAME must be specified")
	}

	relayTo := os.Getenv("TEST_SQS_RELAY_TO")
	if relayTo == "" {
		t.Fatal("TEST_SQS_RELAY_TO must be specified")
	}

	noExistRelayTo := os.Getenv("NOEXIST_TEST_SQS_RELAY_TO")
	if noExistRelayTo == "" {
		t.Fatal("NOEXIST_TEST_SQS_RELAY_TO must be specified")
	}

	drv := &pqDriver{
		workerID: "testing-1",
		db:       newTestDriver(),
	}
	drv.db.Exec("DELETE FROM queue;")
	defer drv.db.Exec("DELETE FROM queue;")

	sqsSvc := sqs.New(&aws.Config{Region: aws.String("ap-northeast-1")})
	q, err := queue.New(sqsSvc, queueName)
	if err != nil {
		t.Fatal(err)
	}

	consumer := NewConsumer(drv.workerID, drv, q)
	relay := NewRelay(sqsSvc)

	sender := NewSender(q)
	if !assert.NoError(sender.SendMessage(5, relayTo, "payload-1")) {
		return
	}
	if !assert.NoError(sender.SendMessage(5, noExistRelayTo, "payload-2")) {
		return
	}

	workerConfig := &WorkerConfig{
		ID: drv.workerID,
	}

	w := NewWorker(workerConfig, drv, consumer, relay)

	var numMessages int64
	for i := 0; i < 2; i++ {
		n, err := w.consume()
		if !assert.NoError(err) {
			return
		}
		numMessages += n
	}
	if !assert.Equal(int64(2), numMessages) {
		return
	}

	time.Sleep(6 * time.Second)
	now := time.Now()
	{
		n, err := w.markActive(now)
		assert.NoError(err)
		assert.Equal(int64(2), n)
	}

	{
		err := w.release()
		assert.NoError(err)
	}

	{
		err := w.release()
		assert.NoError(err)
	}
}

func TestBuildBatchMap(t *testing.T) {
	assert := assert.New(t)

	messages := []*QueueMessage{}
	for i := 0; i < 25; i++ {
		messages = append(messages, &QueueMessage{
			RelayTo: "target-1",
			QueueID: fmt.Sprintf("queue-%d", i),
			Payload: fmt.Sprintf("payload-%d", i),
		})
	}

	messages = append(messages, []*QueueMessage{
		&QueueMessage{
			RelayTo: "target-2",
			QueueID: "queue-26",
			Payload: "payload-26",
		},
		&QueueMessage{
			RelayTo: "target-2",
			QueueID: "queue-27",
			Payload: "payload-27",
		},
		&QueueMessage{
			RelayTo: "target-3",
			QueueID: "queue-28",
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
		assert.Len(actual[expect.RelayTo], len(expect.MessagesLen))

		for i := range actual[expect.RelayTo] {
			assert.Len(actual[expect.RelayTo][i], expect.MessagesLen[i])
			assert.Equal(expect.FirstPayloads[i], actual[expect.RelayTo][i][0].Payload)
		}
	}
}
