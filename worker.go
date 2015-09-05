// delayd2 is an available setTimeout() service for scheduling message sends
package delayd2

import (
	"fmt"
	"log"
	"time"
)

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

	// signaled from external
	shutdownCh chan struct{}

	// notified when handleRelease() finishes its task
	stoppedCh chan struct{}
}

// NewWorker creates a new worker.
func NewWorker(workerID string, driver Driver, consumer *Consumer) *Worker {
	return &Worker{
		id:       workerID,
		driver:   driver,
		consumer: consumer,

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
			log.Printf("worker: %s: unable to consume messages", err)
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
			log.Printf("worker: %s: unable to mark messages active", err)
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
			log.Printf("worker: %s: unable to retrieve messages to be released", err)
			continue
		}

		var n int
		for _, m := range messages {
			fmt.Println(m.Payload)
			if err := w.driver.RemoveMessage(m.QueueID); err != nil {
				log.Printf("worker: %s: unable to remove messages from queue", err)
				continue
			}
			n++
		}

		if n > 0 {
			log.Printf("worker: %d messages removed from queue in %s", n, end.Sub(begin))
		}
	}
}
