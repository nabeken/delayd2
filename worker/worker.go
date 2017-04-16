package worker

import (
	"context"
	"log"
	"runtime"
	"time"

	"github.com/cybozu-go/cmd"
	"github.com/nabeken/delayd2/database"
	"github.com/nabeken/delayd2/queue"
	"github.com/pkg/errors"
)

type Config struct {
	// The number of concurrency for queue access is determined by nCPU * QueueConcurrencyFactor.
	QueueConcurrencyFactor int
	// The number of concurrency for relay is determined by nCPU * QueueConcurrencyFactor.
	RelayConcurrencyFactor int
}

// Worker is the delayd2 worker.
type Worker struct {
	env    *cmd.Environment
	config *Config
	driver database.Driver

	consumer   *queue.Consumer
	manager    *queue.Manager
	dispatcher *queue.Dispatcher
	qservice   *queue.Service
}

// New creates a new worker.
func New(
	e *cmd.Environment,
	c *Config,
	driver database.Driver,
	consumer *queue.Consumer,
	relay *queue.Relay,
	qs *queue.Service,
) *Worker {

	log.Print("worker: initializing a semaphore for the dispatch workers")
	relayConcurrency := runtime.NumCPU() * c.RelayConcurrencyFactor
	dispatcherSem := make(chan struct{}, relayConcurrency)
	for i := 0; i < relayConcurrency; i++ {
		dispatcherSem <- struct{}{}
	}

	return &Worker{
		env:    e,
		config: c,
		driver: driver,

		consumer:   consumer,
		manager:    queue.NewManager(qs),
		dispatcher: queue.NewDispatcher(qs, relay, dispatcherSem),
		qservice:   qs,
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
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	log.Print("worker: initializing the session...")
	if err := w.initializeSession(ctx); err != nil {
		log.Print("worker: unable to initialize the session")
		return err
	}

	queueConcurrency := runtime.NumCPU() * w.config.QueueConcurrencyFactor
	for i := 0; i < queueConcurrency; i++ {
		log.Print("worker: launching consumer worker")
		w.runWorker(w.consumer.Run)
	}

	w.runWorker(w.keepAliveWorker)
	w.runWorker(w.dispatcher.Run)
	w.runWorker(w.manager.Run)

	log.Print("worker: started delayd2 process")

	return nil
}

func (w *Worker) Shutdown(ctx context.Context) error {
	doneCh := make(chan struct{})
	go func() {
		log.Print("worker: waiting for all goroutines to be done...")
		w.env.Wait()

		log.Print("worker: removing cached messages from the database")
		if err := w.qservice.RemoveOngoingMessages(ctx); err != nil {
			log.Print("worker: unable to remove cached messages from the database:", err)
		}

		log.Print("worker: resetting aborted messages remaining in active queue")
		if _, err := w.qservice.ResetActive(ctx); err != nil {
			log.Print("worker: unable to reset messages in active queue:", err)
		}

		log.Print("worker: deregistering delayd2 worker process")
		if err := w.driver.DeregisterSession(ctx); err != nil {
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

func (w *Worker) initializeSession(ctx context.Context) error {
	if err := w.driver.RemoveDeadSession(ctx); err != nil {
		return errors.Wrap(err, "unable to remove the dead session")
	}
	if err := w.driver.RegisterSession(ctx); err != nil {
		return errors.Wrap(err, "unable to register a new session")
	}
	return nil
}

func (w *Worker) keepAliveWorker(ctx context.Context) {
	for {
		select {
		case <-time.Tick(1 * time.Second):
			dctx, cancel := context.WithTimeout(ctx, 10*time.Second)
			if err := w.driver.KeepAliveSession(dctx); err != nil {
				log.Printf("worker: unable to keep alived: %s", err)
			}
			cancel()
		case <-ctx.Done():
			log.Print("worker: shutting down keepalive worker")
			return
		}
	}
}
