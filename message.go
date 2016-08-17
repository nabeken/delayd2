package delayd2

import (
	"log"

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
	_, err := s.queue.SendMessage(payload, option.MessageAttributes(attrs))
	return err
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

		failedIndex := make(map[int]struct{})
		if err := s.queue.SendMessageBatch(batch...); err != nil {
			berrs, batchErr := queue.IsBatchError(err)
			if !batchErr {
				// retry until it succeeds
				log.Printf("unable to send messages but continuing: %s", err)
				continue
			}

			for _, berr := range berrs {
				if berr.SenderFault {
					// return immediately if error is on our fault
					return err
				}
				failedIndex[berr.Index] = struct{}{}
			}
		}
		remaining = remaining[len(cur):]

		// restore failed messages
		for i, m := range batch {
			if _, failed := failedIndex[i]; failed {
				remaining = append(remaining, m.Body)
			}
		}
	}

	return nil
}
