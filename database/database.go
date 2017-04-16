// Package database provides the implementation for the database driver.
package database

import (
	"context"
	"time"
)

// Message is the message queued in the database.
type Message struct {
	ID        string
	WorkerID  string
	ReleaseAt time.Time
	RelayTo   string
	Payload   string
}

// Driver represents the interface for database implementation.
type Driver interface {
	RegisterSession(context.Context) error
	RemoveDeadSession(context.Context) error
	DeregisterSession(context.Context) error
	KeepAliveSession(context.Context) error

	Enqueue(context.Context, string, int64, string, string) error
	RemoveMessages(context.Context, ...string) error

	GetActiveMessages(context.Context) ([]*Message, error)
	MarkActive(context.Context, time.Time) (int64, error)
	ResetActive(context.Context) (int64, error)
}
