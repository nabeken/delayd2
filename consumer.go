package delayd2

import (
	"errors"
	"log"
	"strconv"

	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/nabeken/aws-go-sqs/queue"
	"github.com/nabeken/aws-go-sqs/queue/option"
)

var ErrInvalidAttributes = errors.New("delayd2: invalid attributes")

const (
	sqsMessageDurationKey = "delayd2-delay"
	sqsMessageRelayToKey  = "delayd2-relay-to"
)

// Consumer represents a SQS message consumer.
type Consumer struct {
	workerID string
	driver   Driver
	queue    *queue.Queue
}

func NewConsumer(workerID string, driver Driver, queue *queue.Queue) *Consumer {
	return &Consumer{
		workerID: workerID,
		driver:   driver,
		queue:    queue,
	}
}

// ConsumeMessages consumes messages in SQS queue.
// It returns the number of consumed message in success.
func (c *Consumer) ConsumeMessages() (int, error) {
	messages, err := c.queue.ReceiveMessage(
		option.MaxNumberOfMessages(10),
		option.UseAllAttribute(),
	)
	if err != nil {
		return 0, err
	}

	var n int
	for _, m := range messages {
		duration, relayTo, err := extractDelayd2MessageAttributes(m)
		if err != nil {
			log.Printf("consumer: %s: unable to extract attributes. skipping.", *m.MessageId)
			continue
		}

		err = c.driver.Enqueue(*m.MessageId, duration, relayTo, []byte(*m.Body))
		if err != nil && err != ErrMessageDuplicated {
			log.Printf("consumer: %s: unable to enqueue this message. skipping", *m.MessageId)
			continue
		}
		if err == ErrMessageDuplicated {
			// delete immediately if duplicated
			log.Printf("consumer: %s: %s", *m.MessageId, err)
		}

		log.Printf("consumer: %s: enqueued", *m.MessageId)
		c.queue.DeleteMessage(m.ReceiptHandle)
		n++
	}
	return n, nil
}

func extractDelayd2MessageAttributes(message *sqs.Message) (int64, string, error) {
	var duration int64
	var relayTo string
	var err error

	durationAttr, found := message.MessageAttributes[sqsMessageDurationKey]
	if !found || *durationAttr.DataType != option.DataTypeNumber {
		return 0, "", ErrInvalidAttributes
	}
	duration, err = strconv.ParseInt(*durationAttr.StringValue, 10, 64)
	if err != nil {
		return 0, "", err
	}

	relayToAttr, found := message.MessageAttributes[sqsMessageRelayToKey]
	if !found || *relayToAttr.DataType != option.DataTypeString {
		return 0, "", ErrInvalidAttributes
	}
	relayTo = *relayToAttr.StringValue

	return duration, relayTo, nil
}
