package queue

import (
	"context"
	"log"
	"time"
)

// Manager manages the queue in the database.
type Manager struct {
	qservice *Service
}

func NewManager(qs *Service) *Manager {
	return &Manager{
		qservice: qs,
	}
}

func (m *Manager) Run(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			log.Print("manager: shutting down the queue manager worker")
			return
		case <-time.Tick(100 * time.Millisecond):
			begin := time.Now().Truncate(time.Second)
			n, err := m.qservice.MarkActive(ctx, begin)
			end := time.Now()
			if err != nil {
				log.Print("manager: unable to move to active queue")
			}
			if n > 0 {
				log.Printf("worker: %d messages marked as active in %s", n, end.Sub(begin))
			}
		}
	}
}
