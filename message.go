package delayd2

import (
	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/nabeken/aws-go-sqs/queue"
	"github.com/nabeken/aws-go-sqs/queue/option"
)

type Sender struct {
	sqsSvc *sqs.SQS
}

func (s *Sender) SendMessage(queueName string, duration int, relayTo, payload string) error {
	attrs := map[string]interface{}{
		sqsMessageDurationKey: duration,
		sqsMessageRelayToKey:  relayTo,
	}
	q, err := queue.New(s.sqsSvc, queueName)
	if err != nil {
		return err
	}

	return q.SendMessage(payload, option.MessageAttributes(attrs))
}
