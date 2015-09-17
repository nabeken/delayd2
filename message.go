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

func (s *Sender) SendMessageBatch(duration int, relayTo string, payloads []string) error {
	attrs := map[string]interface{}{
		sqsMessageDurationKey: duration,
		sqsMessageRelayToKey:  relayTo,
	}

	remaining := payloads
	var cur []string
	for {
		if len(remaining) == 0 {
			break
		}

		if len(remaining) > 10 {
			cur = remaining[:10]
		} else {
			cur = remaining
		}

		batch := make([]queue.BatchMessage, 0, len(cur))

		for _, body := range cur {
			batch = append(batch, queue.BatchMessage{
				Body:    body,
				Options: []option.SendMessageInput{option.MessageAttributes(attrs)},
			})
		}

		// stop immediately if error occurs
		if err := s.queue.SendMessageBatch(batch...); err != nil {
			return err
		}
		remaining = remaining[len(cur):]
	}

	return nil
}
