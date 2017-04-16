package queue

import (
	"context"
	"log"
	"sync"
	"time"

	sqsqueue "github.com/nabeken/aws-go-sqs/queue"
	"github.com/nabeken/delayd2/database"
)

type Dispatcher struct {
	qservice *Service
	relay    *Relay

	// semaphore for dispatch workers
	dispatchSem chan struct{}
}

func NewDispatcher(qs *Service, relay *Relay, sem chan struct{}) *Dispatcher {
	return &Dispatcher{
		qservice: qs,
		relay:    relay,

		dispatchSem: sem,
	}
}

type Message struct {
	QueueID string
	Payload string
}

type Job struct {
	RelayTo  string
	Messages []Message
}

func (d *Dispatcher) Run(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			log.Print("dispatcher: shutting down the release dispatcher")
			return
		case <-time.Tick(10 * time.Millisecond):
			messages, err := d.qservice.GetActiveMessages(ctx)
			if err != nil {
				log.Printf("dispatcher: unable to get active messages: %s", err)
				continue
			}

			if len(messages) == 0 {
				continue
			}

			// we need QueueID to delete from the queue in the database
			batchMap := BuildBatchMap(messages)
			var wg sync.WaitGroup
			for relayTo, batchMsgs := range batchMap {
				for _, rms := range batchMsgs {
					<-d.dispatchSem
					job := &Job{
						RelayTo:  relayTo,
						Messages: rms,
					}
					wg.Add(1)
					go func(job *Job) {
						defer wg.Done()
						d.Dispatch(ctx, job)
						d.dispatchSem <- struct{}{}
					}(job)
				}
			}

			// we must wait for all active messages to be released.
			wg.Wait()
		}
	}
}

func (d *Dispatcher) Dispatch(ctx context.Context, job *Job) {
	payloads := make([]string, 0, len(job.Messages))
	for _, m := range job.Messages {
		if d.qservice.IsMessageDone(m.QueueID) {
			// skip since we already dispatched
			// but we need to try to remove it from the database
			log.Printf("dispatcher: %s is in the cache so continueing...", m.QueueID)

			if err := d.qservice.RemoveMessages(ctx, m.QueueID); err != nil {
				log.Printf("dispatcher: unable to remove %s in the database but continue.", m.QueueID)
			} else {
				log.Printf("dispatcher: %s has been removed in the database.", m.QueueID)
			}
			continue
		}
		payloads = append(payloads, m.Payload)
	}

	failedIndex := make(map[int]struct{})
	if len(payloads) > 0 {
		if err := d.relay.Relay(job.RelayTo, payloads); err != nil {
			// only print log here in batch operation
			// TODO: a dead letter queue support
			berrs, batchOk := sqsqueue.IsBatchError(err)
			if !batchOk {
				log.Printf("worker: unable to send message due to non batch error. skipping: %s", err)
				return
			}

			for _, berr := range berrs {
				if berr.SenderFault {
					log.Printf("worker: unable to send message due to sender's fault: skipping: %s", berr.Message)
				}
				failedIndex[berr.Index] = struct{}{}
			}
		}
	}

	succededIDs := make([]string, 0, len(job.Messages))
	for i, m := range job.Messages {
		if _, failed := failedIndex[i]; failed {
			continue
		}
		succededIDs = append(succededIDs, m.QueueID)

		// remember succededIDs for a while to prevent us from relaying message in transient error
		d.qservice.MarkMessgeDone(m.QueueID)
	}

	var n int64
	if err := d.qservice.RemoveMessages(ctx, succededIDs...); err != nil {
		log.Printf("worker: unable to remove messages from the database: %s", err)
	} else {
		n += int64(len(succededIDs))
	}
	log.Printf("worker: %d messages removed from the database", n)
}

func BuildBatchMap(messages []*database.Message) map[string][][]Message {
	batchMap := make(map[string][][]Message)
	for _, m := range messages {
		msg := Message{
			QueueID: m.ID,
			Payload: m.Payload,
		}

		var i int
		i = len(batchMap[m.RelayTo])
		if i == 0 || len(batchMap[m.RelayTo][i-1]) >= 10 {
			batchMap[m.RelayTo] = append(batchMap[m.RelayTo], []Message{})
		}

		i = len(batchMap[m.RelayTo])

		batchMap[m.RelayTo][i-1] = append(batchMap[m.RelayTo][i-1], msg)
	}
	return batchMap
}
