package worker

import (
	"context"
	"log"
	"runtime"
	"sync"
	"time"

	"github.com/cybozu-go/cmd"
	sqsqueue "github.com/nabeken/aws-go-sqs/queue"
	"github.com/nabeken/delayd2/database"
	"github.com/nabeken/delayd2/queue"
	"github.com/pmylund/go-cache"
)

// Worker is the delayd2 worker.
type Worker struct {
	env      *cmd.Environment
	driver   database.Driver
	consumer *queue.Consumer
	relay    *queue.Relay

	succededIDsCache *cache.Cache

	// semaphore for release workers
	releaseSem  chan struct{}
	releaseDone chan struct{}
}

// New creates a new worker.
func New(e *cmd.Environment, driver database.Driver, consumer *queue.Consumer, relay *queue.Relay) *Worker {
	return &Worker{
		env:      e,
		driver:   driver,
		consumer: consumer,
		relay:    relay,

		succededIDsCache: cache.New(15*time.Minute, 1*time.Minute),

		// should be buffered not to block releaseDispatcher()
		releaseDone: make(chan struct{}, 1),
	}
}

func (w *Worker) runWorker(f func(ctx context.Context)) {
	w.env.Go(func(ctx context.Context) error {
		f(ctx)
		return nil
	})
}

// Run starts the worker process. It is not blocked.
func (w *Worker) Run() error {
	log.Print("worker: registering delayd2 worker process")
	if err := w.driver.RegisterSession(); err != nil {
		return err
	}

	nCPU := runtime.NumCPU()
	for i := 0; i < nCPU; i++ {
		log.Print("worker: launching consumer worker")
		w.runWorker(w.consumeWorker)
	}

	// initializing a semaphore for release workers
	log.Print("worker: initializing a semaphore for release workers")
	w.releaseSem = make(chan struct{}, nCPU)
	for i := 0; i < nCPU; i++ {
		w.releaseSem <- struct{}{}
	}

	w.runWorker(w.keepAliveWorker)
	w.runWorker(w.markActiveWorker)
	w.runWorker(w.releaseDispatcher)

	log.Print("worker: started delayd2 process")

	return nil
}

func (w *Worker) Shutdown(ctx context.Context) error {
	doneCh := make(chan struct{})
	go func() {
		log.Print("worker: waiting for all goroutines to be done...")
		w.env.Wait()

		log.Print("worker: removing cached messages from the database")
		if err := w.removeOngoingMessages(ctx); err != nil {
			log.Print("worker: unable to remove cached messages from the database:", err)
		}

		log.Print("worker: resetting aborted messages remaining in active queue")
		if _, err := w.driver.ResetActive(); err != nil {
			log.Print("worker: unable to reset messages in active queue:", err)
		}

		log.Print("worker: deregistering delayd2 worker process")
		if err := w.driver.DeregisterSession(); err != nil {
			log.Print("worker: unable to deregister session:", err)
		}

		close(doneCh)
	}()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-doneCh:
	}

	log.Print("worker: the process finished.")

	return nil
}

func (w *Worker) keepAliveWorker(ctx context.Context) {
	for {
		select {
		case <-time.Tick(1 * time.Second):
			if err := w.driver.KeepAliveSession(); err != nil {
				log.Printf("worker: unable to keep alived: %s", err)
			}
		case <-ctx.Done():
			log.Print("worker: shutting down keepalive worker")
			return
		}
	}
}

// consumeWorker consumes messages in SQS.
func (w *Worker) consumeWorker(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			log.Print("worker: shutting down consuming worker")
			return
		default:
		}

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

// markActiveWorker marks coming messages in queue.
func (w *Worker) markActiveWorker(ctx context.Context) {
	for {
		select {
		case <-w.releaseDone:
			w.markActive()
		case <-time.Tick(1 * time.Second):
			w.markActive()
		case <-ctx.Done():
			log.Print("worker: shutting down marking worker")
			return
		}
	}
}

func (w *Worker) markActive() {
	begin := time.Now().Truncate(time.Second)
	n, err := w.driver.MarkActive(begin)
	end := time.Now()

	if err != nil {
		log.Printf("worker: unable to mark messages active: %s", err)
		return
	}

	if n > 0 {
		log.Printf("worker: %d messages marked as active in %s", n, end.Sub(begin))
	}
}

type releaseJob struct {
	relayTo  string
	messages []releaseMessage
}

// releaseDispatcher dispatches relay jobs to release workers.
func (w *Worker) releaseDispatcher(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			log.Print("worker: shutting down release dispatcher")
			return
		case <-time.Tick(10 * time.Millisecond):
			messages, err := w.driver.GetActiveMessages()
			if err != nil {
				log.Printf("worker: unable to get active messages: %s", err)
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
					<-w.releaseSem
					job := &releaseJob{
						relayTo:  relayTo,
						messages: rms,
					}
					wg.Add(1)
					go func(job *releaseJob) {
						defer wg.Done()
						w.release(job)
						w.releaseSem <- struct{}{}
					}(job)
				}
			}

			// we must wait for all active messages to be released.
			wg.Wait()

			// let markActiveWorker know we're done
			w.releaseDone <- struct{}{}
		}
	}
}

type releaseMessage struct {
	QueueID string
	Payload string
}

func (w *Worker) release(job *releaseJob) {
	payloads := make([]string, 0, len(job.messages))
	for _, m := range job.messages {
		if _, found := w.succededIDsCache.Get(m.QueueID); found {
			// skip since we already relayed
			// but we need to try to remove it from the database
			log.Printf("worker: %s is in the cache so continueing...", m.QueueID)

			if err := w.removeMessages(m.QueueID); err != nil {
				log.Printf("worker: unable to remove %s in the database but continue.", m.QueueID)
			} else {
				log.Printf("worker: %s has been removed in the database.", m.QueueID)
			}
			continue
		}
		payloads = append(payloads, m.Payload)
	}

	failedIndex := make(map[int]struct{})
	if len(payloads) > 0 {
		if err := w.relay.Relay(job.relayTo, payloads); err != nil {
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

	succededIDs := make([]string, 0, len(job.messages))
	for i, m := range job.messages {
		if _, failed := failedIndex[i]; failed {
			continue
		}
		succededIDs = append(succededIDs, m.QueueID)

		// remember succededIDs for a while to prevent us from relaying message in transient error
		w.succededIDsCache.Set(m.QueueID, struct{}{}, cache.DefaultExpiration)
	}

	var n int64
	if err := w.removeMessages(succededIDs...); err != nil {
		log.Printf("worker: unable to remove messages from the database: %s", err)
	} else {
		n += int64(len(succededIDs))
	}
	log.Printf("worker: %d messages removed from the database", n)
}

func (w *Worker) removeMessages(ids ...string) error {
	if err := w.driver.RemoveMessages(ids...); err != nil {
		return err
	}

	// reset cache here since we succeeded to remove messages from the database.
	// When we succeeded to remove messages, the messages will not appear again so it's safe.
	for _, id := range ids {
		w.succededIDsCache.Delete(id)
	}

	return nil
}

// removeOngoingMessages tries to remove all messages in the cache from the database.
func (w *Worker) removeOngoingMessages(ctx context.Context) error {
	for {
		cachedIDs := w.succededIDsCache.Items()

		if len(cachedIDs) == 0 {
			log.Print("worker: all cached messages have been removed from the database")
			return nil
		}

		log.Printf("worker: removing %d cached but not removed from the database...", len(cachedIDs))

		ids := make([]string, 0, len(cachedIDs))

		for id := range cachedIDs {
			ids = append(ids, id)
		}

		if err := w.removeMessages(ids...); err != nil {
			log.Printf("worker: unable to remove from the database. retrying...")
			time.Sleep(1 * time.Second)
		}

		select {
		case <-ctx.Done():
			log.Print("worker: giving up removing from the database...")
			return ctx.Err()
		default:
		}
	}

	return nil
}

func BuildBatchMap(messages []*database.Message) map[string][][]releaseMessage {
	batchMap := make(map[string][][]releaseMessage)
	for _, m := range messages {
		rm := releaseMessage{
			QueueID: m.ID,
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
