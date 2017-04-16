package queue

import (
	"context"
	"log"
	"time"

	"github.com/nabeken/delayd2/database"
	cache "github.com/pmylund/go-cache"
)

// Service is the queue manipulation service.
type Service struct {
	driver database.Driver
	cache  *cache.Cache
}

func NewService(driver database.Driver) *Service {
	return &Service{
		driver: driver,
		cache:  cache.New(15*time.Minute, 1*time.Minute),
	}
}

func (s *Service) RemoveMessages(ctx context.Context, ids ...string) error {
	if err := s.driver.RemoveMessages(ctx, ids...); err != nil {
		return err
	}

	// reset cache here since we succeeded to remove messages from the database.
	// When we succeeded to remove messages, the messages will not appear again so it's safe.
	for _, id := range ids {
		s.cache.Delete(id)
	}

	return nil
}

func (s *Service) GetActiveMessages(ctx context.Context) ([]*database.Message, error) {
	return s.driver.GetActiveMessages(ctx)
}

func (s *Service) ResetActive(ctx context.Context) (int64, error) {
	return s.driver.ResetActive(ctx)
}

func (s *Service) MarkActive(ctx context.Context, begin time.Time) (int64, error) {
	return s.driver.MarkActive(ctx, begin)
}

func (s *Service) Enqueue(ctx context.Context, queueID string, duration int64, relayTo, body string) error {
	return s.driver.Enqueue(ctx, queueID, duration, relayTo, body)
}

func (s *Service) MarkMessgeDone(queueID string) {
	s.cache.Set(queueID, struct{}{}, cache.DefaultExpiration)
}

func (s *Service) IsMessageDone(queueID string) bool {
	_, found := s.cache.Get(queueID)
	return found
}

// RemoveOngoingMessages tries to remove all messages in the cache from the database.
func (s *Service) RemoveOngoingMessages(ctx context.Context) error {
	for {
		cachedIDs := s.cache.Items()

		if len(cachedIDs) == 0 {
			log.Print("queue: all cached messages have been removed from the database")
			return nil
		}

		log.Printf("queue: removing %d cached but not removed from the database...", len(cachedIDs))

		ids := make([]string, 0, len(cachedIDs))

		for id := range cachedIDs {
			ids = append(ids, id)
		}

		if err := s.RemoveMessages(ctx, ids...); err != nil {
			log.Printf("queue: unable to remove from the database. retrying...")
			time.Sleep(1 * time.Second)
		}

		select {
		case <-ctx.Done():
			log.Print("queue: giving up removing from the database...")
			return ctx.Err()
		default:
		}
	}
}
