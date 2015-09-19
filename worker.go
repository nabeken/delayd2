// delayd2 is an available setTimeout() service for scheduling message sends
package delayd2

import (
	"log"
	"time"

	"github.com/nabeken/aws-go-sqs/queue"
)

// QueueMessage is a message queued in the database.
type QueueMessage struct {
	QueueID   string
	WorkerID  string
	ReleaseAt time.Time
	RelayTo   string
	Payload   string
}

// Worker is the delayd2 worker.
type Worker struct {
	id       string
	driver   Driver
	consumer *Consumer
	relay    *Relay

	// signaled from external
	shutdownCh chan struct{}

	// notified when handleRelease() finishes its task
	stoppedCh chan struct{}
}

// NewWorker creates a new worker.
func NewWorker(workerID string, driver Driver, consumer *Consumer, relay *Relay) *Worker {
	return &Worker{
		id:       workerID,
		driver:   driver,
		consumer: consumer,
		relay:    relay,

		shutdownCh: make(chan struct{}),
		stoppedCh:  make(chan struct{}),
	}
}

// Run starts the worker process.
func (w *Worker) Run() error {
	log.Print("worker: starting delayd2 worker process")

	log.Print("worker: resetting aborted messages remaining in active queue")
	n, err := w.driver.ResetActive()
	if err != nil {
		return err
	}
	log.Printf("worker: %d aborted messages in active queue resetted", n)

	log.Print("worker: starting delayd2 process")
	go w.handleConsume()
	go w.handleMarkActive()
	go w.handleRelease()

	select {
	case <-w.shutdownCh:
		log.Print("worker: receiving shutdown signal. waiting for the process finished.")
	}

	select {
	case <-w.stoppedCh:
		log.Print("worker: the process finished.")
	}
	return nil
}

func (w *Worker) Stop() {
	close(w.shutdownCh)
}

func (w *Worker) consume() (int64, error) {
	return w.consumer.ConsumeMessages()
}

// handleConsume consumes messages in SQS.
func (w *Worker) handleConsume() {
	for {
		begin := time.Now()
		n, err := w.consume()
		end := time.Now()

		if err != nil {
			log.Printf("worker: unable to consume messages: %s", err)
			continue
		}

		if n > 0 {
			log.Printf("worker: %d messages consumed in %s", n, end.Sub(begin))
		}
	}
}

func (w *Worker) markActive(begin time.Time) (int64, error) {
	return w.driver.MarkActive(begin)
}

// handleMarkActive marks coming messages in queue.
func (w *Worker) handleMarkActive() {
	for range time.Tick(1 * time.Second) {
		begin := time.Now()

		n, err := w.markActive(begin)

		end := time.Now()

		if err != nil {
			log.Printf("worker: unable to mark messages active: %s", err)
			continue
		}

		if n > 0 {
			log.Printf("worker: %d messages marked as active in %s", n, end.Sub(begin))
		}
	}
}

// handleRelease releases messages that is ready to fire.
func (w *Worker) handleRelease() {
	for range time.Tick(1 * time.Second) {
		select {
		case <-w.shutdownCh:
			close(w.stoppedCh)
			return
		default:
		}

		begin := time.Now()

		n, err := w.release()
		if err != nil {
			log.Printf("worker: unable to release messages: %s", err)
			continue
		}

		end := time.Now()

		if n > 0 {
			log.Printf("worker: %d messages removed from queue in %s", n, end.Sub(begin))
		}
	}
}

// release releases active messages. It returns the number of released message n.
func (w *Worker) release() (int64, error) {
	messages, err := w.driver.GetActiveMessages()

	if err != nil {
		return 0, err
	}

	var n int64
	messagesMap := make(map[string][]*QueueMessage)
	for _, m := range messages {
		messagesMap[m.RelayTo] = append(messagesMap[m.RelayTo], m)
	}

	for r, ms := range messagesMap {
		// FIXME: should be in parallel
		payloads := make([]string, 0, len(ms))
		for _, m := range ms {
			payloads = append(payloads, m.Payload)
		}

		failedIndex := make(map[int]struct{})
		err := w.relay.Relay(payloads, r)
		if err != nil {
			// only print log here in batch operation
			// TODO: a dead letter queue support
			berrs, batchOk := queue.IsBatchError(err)
			if !batchOk {
				log.Printf("worker: unable to send message due to non batch error. skipping: %s", err)
				continue
			}

			for _, berr := range berrs {
				if berr.SenderFault {
					log.Printf("worker: unable to send message due to sender's fault: skipping: %s", berr.Message)
				}
				failedIndex[berr.Index] = struct{}{}
			}
		}

		for i, m := range ms {
			if _, failed := failedIndex[i]; failed {
				continue
			}

			if err := w.driver.RemoveMessage(m.QueueID); err != nil {
				log.Printf("worker: unable to remove messages from queue: %s", err)
				continue
			}
			n++
		}
	}
	return n, nil
}
