package delayd2

import (
	"github.com/nabeken/aws-go-sqs/queue"
	"github.com/nabeken/aws-go-sqs/queue/option"
)

type Sender struct {
	queue *queue.Queue
}

func NewSender(queue *queue.Queue) *Sender {
	return &Sender{
		queue: queue,
	}
}

func (s *Sender) SendMessage(duration int, relayTo, payload string) error {
	attrs := map[string]interface{}{
		sqsMessageDurationKey: duration,
		sqsMessageRelayToKey:  relayTo,
	}
	return s.queue.SendMessage(payload, option.MessageAttributes(attrs))
}
