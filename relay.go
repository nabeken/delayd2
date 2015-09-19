package delayd2

import (
	"errors"
	"sync"

	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/nabeken/aws-go-sqs/queue"
)

var ErrTooManyPayloads = errors.New("relay: too many payloads. Up to 10 payloads can be sent at once")

type Relay struct {
	sqsSvc *sqs.SQS

	// maps queue name to queue url
	mu       sync.RWMutex
	queueMap map[string]*string
}

func NewRelay(sqsSvc *sqs.SQS) *Relay {
	return &Relay{
		sqsSvc: sqsSvc,

		queueMap: make(map[string]*string),
	}
}

func (r *Relay) existURL(relayTo string) (*string, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	u, found := r.queueMap[relayTo]
	return u, found
}

func (r *Relay) retrieveURL(relayTo string) (*string, error) {
	if u, found := r.existURL(relayTo); found {
		return u, nil
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	u, err := queue.GetQueueURL(r.sqsSvc, relayTo)
	if err != nil {
		return nil, err
	}
	r.queueMap[relayTo] = u
	return u, nil
}

// Relay relays payloads to relayTo queue.
func (r *Relay) Relay(relayTo string, payloads []string) error {
	if len(payloads) > 10 {
		return ErrTooManyPayloads
	}

	url, err := r.retrieveURL(relayTo)
	if err != nil {
		return err
	}

	batch := make([]queue.BatchMessage, 0, len(payloads))
	for _, body := range payloads {
		batch = append(batch, queue.BatchMessage{
			Body: body,
		})
	}

	entries, id2index := queue.BuildBatchRequestEntry(batch...)
	req := &sqs.SendMessageBatchInput{
		Entries:  entries,
		QueueUrl: url,
	}

	return SendMessageBatch(r.sqsSvc, req, id2index)
}

func SendMessageBatch(s *sqs.SQS, req *sqs.SendMessageBatchInput, id2index map[string]int) error {
	resp, err := s.SendMessageBatch(req)
	if err != nil {
		return err
	}
	return queue.NewBatchError(id2index, resp.Failed)
}
