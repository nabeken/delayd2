package delayd2

import (
	"database/sql"
	"errors"
	"strconv"
	"time"

	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/lib/pq"
	"github.com/nabeken/aws-go-sqs/queue"
	"github.com/nabeken/aws-go-sqs/queue/option"
	"github.com/pborman/uuid"
)

var ErrInvalidAttributes = errors.New("delayd2: invalid attributes")

const (
	sqsMessageDurationKey = "delayd2-delay"
	sqsMessageRelayToKey  = "delayd2-target"
)

type SQSConsumer struct {
	workerID string
	db       *sql.DB
	queue    *queue.Queue
}

func NewSQSConsumer(workerID string, db *sql.DB, svc *sqs.SQS, queueName string) (*SQSConsumer, error) {
	q, err := queue.New(svc, queueName)
	if err != nil {
		return nil, err
	}
	return &SQSConsumer{
		workerID: workerID,
		db:       db,
		queue:    q,
	}, nil
}

func (c *SQSConsumer) Enqueue(delay int64, relayTo string, payload []byte) error {
	releaseAt := time.Now().Add(time.Duration(delay) * time.Second)
	queueId := uuid.New()
	_, err := c.db.Exec(`
		INSERT
		INTO
		  queue
		VALUES
			($1, $2, $3, $4, $5)
	;`, queueId, c.workerID, releaseAt, relayTo, payload)
	return err
}

func (c *SQSConsumer) ConsumeMessages() error {
	messages, err := c.queue.ReceiveMessage(
		option.MaxNumberOfMessages(10),
		option.UseAllAttribute(),
	)
	if err != nil {
		return err
	}
	for _, m := range messages {
		duration, relayTo, err := extractDelayd2MessageAttributes(m)
		if err != nil {
			// TODO: log
			continue
		}
		if err := c.Enqueue(duration, relayTo, []byte(*m.Body)); err != nil {
			if perr, ok := err.(*pq.Error); ok && perr.Code.Class() == "23" {
				// delete immediately if duplicated
				c.queue.DeleteMessage(m.ReceiptHandle)
			}
			// TODO: log
			continue
		}
		c.queue.DeleteMessage(m.ReceiptHandle)
	}
	return nil
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
