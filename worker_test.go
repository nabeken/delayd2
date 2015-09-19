package delayd2

import (
	"flag"
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

	w := NewWorker(drv.workerID, drv, consumer, relay)

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
		n, err := w.release()
		assert.NoError(err)

		// one of messages has incorrect relayTo so we could release only 1 message
		assert.Equal(int64(1), n)
	}

	{
		n, err := w.release()
		assert.NoError(err)

		// no messages released
		assert.Equal(int64(0), n)
	}
}
