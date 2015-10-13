// delayd2 is an available setTimeout() service for scheduling message sends
package delayd2

import (
	"log"
	"runtime"
	"sync"
	"time"

	"github.com/nabeken/aws-go-sqs/queue"
	"github.com/pmylund/go-cache"
)

// QueueMessage is a message queued in the database.
type QueueMessage struct {
	QueueID   string
	WorkerID  string
	ReleaseAt time.Time
	RelayTo   string
	Payload   string
}

// WorkerConfig is configuration for Worker.
type WorkerConfig struct {
	ID string

	MarkOrphanedAtShutdown bool

	NumConsumerFactor int
	NumRelayFactor    int
}

// Worker is the delayd2 worker.
type Worker struct {
	config *WorkerConfig

	driver   Driver
	consumer *Consumer
	relay    *Relay

	succededIDsCache *cache.Cache

	relayCh chan releaseJob

	// signaled from external
	shutdownCh chan struct{}

	// notified when handleRelease() finishes its task
	stoppedCh chan struct{}
}

// NewWorker creates a new worker.
func NewWorker(c *WorkerConfig, driver Driver, consumer *Consumer, relay *Relay) *Worker {
	return &Worker{
		driver:   driver,
		consumer: consumer,
		relay:    relay,

		// Cache succededIDs to prepare for transient errors in the database (e.g. failover/network problems)
		succededIDsCache: cache.New(15*time.Minute, 1*time.Minute),

		config: c,

		relayCh:    make(chan releaseJob, runtime.NumCPU()*c.NumRelayFactor),
		shutdownCh: make(chan struct{}),
		stoppedCh:  make(chan struct{}),
	}
}

// Run starts the worker process.
func (w *Worker) Run() error {
	log.Print("worker: starting delayd2 worker process")

	log.Print("worker: registering delayd2 worker process")
	if err := w.driver.RegisterSession(); err != nil {
		return err
	}

	defer func() {
		if w.config.MarkOrphanedAtShutdown {
			log.Print("worker: marking messages as orphaned")

			if err := w.driver.MarkOrphaned(); err != nil {
				log.Print("worker: unable to mark messages as orphaned:", err)
			}
		}

		log.Print("worker: deregistering delayd2 worker process")
		if err := w.driver.DeregisterSession(); err != nil {
			log.Print("worker: unable to deregister session:", err)
		}
	}()

	log.Print("worker: resetting aborted messages remaining in active queue")
	n, err := w.driver.ResetActive()
	if err != nil {
		return err
	}
	log.Printf("worker: %d aborted messages in active queue resetted", n)

	log.Print("worker: starting delayd2 process")

	nCPU := runtime.NumCPU()
	for i := 0; i < nCPU*w.config.NumConsumerFactor; i++ {
		log.Print("worker: launching consumer process")
		go w.handleConsume()
	}

	for i := 0; i < nCPU*w.config.NumRelayFactor; i++ {
		log.Print("worker: launching relay worker process")
		go w.relayWorker()
	}

	go w.handleAdoptOrphans()
	go w.handleKeepAlive()
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

func (w *Worker) handleKeepAlive() {
	for range time.Tick(1 * time.Second) {
		if err := w.driver.KeepAliveSession(); err != nil {
			log.Printf("worker: unable to keep alived: %s", err)
		}
	}
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

func (w *Worker) handleAdoptOrphans() {
	for range time.Tick(1 * time.Minute) {
		begin := time.Now()

		n, err := w.driver.AdoptOrphans()

		end := time.Now()

		if err != nil {
			log.Printf("worker: unable to adopt orphans: %s", err)
			continue
		}

		if n > 0 {
			log.Printf("worker: %d orphans adopted in %s", n, end.Sub(begin))
		}
	}
}

func (w *Worker) markActive(begin time.Time) (int64, error) {
	return w.driver.MarkActive(begin)
}

// handleMarkActive marks coming messages in queue.
func (w *Worker) handleMarkActive() {
	for range time.Tick(10 * time.Millisecond) {
		begin := time.Now().Truncate(time.Second)

		n, err := w.markActive(begin)

		end := time.Now()

		if err != nil {
			log.Printf("worker: unable to mark messages active: %s", err)
			continue
		}

		if n > 0 {
			log.Printf("worker: %d messages marked as active in %s", n, end.Sub(begin))
		}
		time.Sleep(1 * time.Second)
	}
}

// handleRelease releases messages that is ready to fire.
func (w *Worker) handleRelease() {
	for range time.Tick(10 * time.Millisecond) {
		select {
		case <-w.shutdownCh:
			close(w.relayCh)
			close(w.stoppedCh)
			return
		default:
		}

		if err := w.release(); err != nil {
			log.Printf("worker: unable to release messages: %s", err)
			continue
		}
	}
}

type releaseJob struct {
	*sync.WaitGroup

	RelayTo  string
	Messages []releaseMessage
}

type releaseMessage struct {
	QueueID string
	Payload string
}

// release releases active messages. It returns the number of released message n.
func (w *Worker) release() error {
	messages, err := w.driver.GetActiveMessages()

	if err != nil {
		return err
	}

	// we need QueueID to delete from queue
	batchMap := BuildBatchMap(messages)

	var wg sync.WaitGroup
	for relayTo, batchMsgs := range batchMap {
		for _, rms := range batchMsgs {
			wg.Add(1)
			w.relayCh <- releaseJob{
				WaitGroup: &wg,

				RelayTo:  relayTo,
				Messages: rms,
			}
		}
	}
	wg.Wait()
	return nil
}

func (w *Worker) relayWorker() {
	for rj := range w.relayCh {
		begin := time.Now()
		n := w.releaseBatch(rj)
		end := time.Now()

		if n > 0 {
			log.Printf("worker: %d messages relayed in %s", n, end.Sub(begin))
		}
		rj.Done()
	}
	log.Print("relayworker: closed")
}

func (w *Worker) releaseBatch(rj releaseJob) int64 {
	payloads := make([]string, 0, len(rj.Messages))
	for _, m := range rj.Messages {
		if _, found := w.succededIDsCache.Get(m.QueueID); found {
			// skip since we already relayed
			log.Printf("worker: seeing queue_id in the cache so continueing...")
			continue
		}
		payloads = append(payloads, m.Payload)
	}

	var n int64
	failedIndex := make(map[int]struct{})
	if len(payloads) > 0 {
		if err := w.relay.Relay(rj.RelayTo, payloads); err != nil {
			// only print log here in batch operation
			// TODO: a dead letter queue support
			berrs, batchOk := queue.IsBatchError(err)
			if !batchOk {
				log.Printf("worker: unable to send message due to non batch error. skipping: %s", err)
				return 0
			}

			for _, berr := range berrs {
				if berr.SenderFault {
					log.Printf("worker: unable to send message due to sender's fault: skipping: %s", berr.Message)
				}
				failedIndex[berr.Index] = struct{}{}
			}
		}
	}

	succededIDs := make([]string, 0, len(rj.Messages))
	for i, m := range rj.Messages {
		if _, failed := failedIndex[i]; failed {
			continue
		}
		succededIDs = append(succededIDs, m.QueueID)

		// remember succededIDs for a while to prevent us from relaying message in transient error
		w.succededIDsCache.Set(m.QueueID, struct{}{}, cache.DefaultExpiration)
	}

	if err := w.driver.RemoveMessages(succededIDs); err != nil {
		log.Printf("worker: unable to remove messages from queue: %s", err)
	} else {
		// reset succededIDs here since we succeeded to remove messages from the database
		// When we succeeded to remove messages, the messages will not appear again so it's safe.
		for _, id := range succededIDs {
			w.succededIDsCache.Delete(id)
		}
		n += int64(len(succededIDs))
	}

	return n
}

func BuildBatchMap(messages []*QueueMessage) map[string][][]releaseMessage {
	batchMap := make(map[string][][]releaseMessage)
	for _, m := range messages {
		rm := releaseMessage{
			QueueID: m.QueueID,
			Payload: m.Payload,
		}

		var i int
		i = len(batchMap[m.RelayTo])
		if i == 0 || len(batchMap[m.RelayTo][i-1]) >= 10 {
			batchMap[m.RelayTo] = append(batchMap[m.RelayTo], []releaseMessage{})
		}

		i = len(batchMap[m.RelayTo])

		batchMap[m.RelayTo][i-1] = append(batchMap[m.RelayTo][i-1], rm)
	}
	return batchMap
}
