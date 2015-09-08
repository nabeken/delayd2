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
	go w.handleIncoming()
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

// handleIncoming consumes messages in SQS.
func (w *Worker) handleIncoming() {
	for {
		begin := time.Now()
		n, err := w.consumer.ConsumeMessages()
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

// handleMarkActive marks coming messages in queue.
func (w *Worker) handleMarkActive() {
	for range time.Tick(1 * time.Second) {
		begin := time.Now()

		n, err := w.driver.MarkActive(begin)

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
		messages, err := w.driver.GetActiveMessages()
		end := time.Now()

		if err != nil {
			log.Printf("worker: unable to retrieve messages to be released: %s", err)
			continue
		}

		var n int
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
				if berrs, ok := queue.IsBatchError(err); ok {
					for _, berr := range berrs {
						if berr.SenderFault {
							// TODO: remove? log?
							log.Printf("worker: unable to send message batch due to sender's fault: %s", berr.Message)
						}
						failedIndex[berr.Index] = struct{}{}
					}
				}
			}

			for i, m := range ms {
				_, failed := failedIndex[i]
				if failed {
					continue
				}

				if err := w.driver.RemoveMessage(m.QueueID); err != nil {
					log.Printf("worker: unable to remove messages from queue: %s", err)
					continue
				}
				n++
			}
		}

		if n > 0 {
			log.Printf("worker: %d messages removed from queue in %s", n, end.Sub(begin))
		}
	}
}
