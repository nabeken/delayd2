// Package database provides the implementation for the database driver.
package database

import "time"

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
	RegisterSession() error
	DeregisterSession() error
	KeepAliveSession() error

	Enqueue(string, int64, string, string) error
	RemoveMessages(...string) error

	GetActiveMessages() ([]*Message, error)
	MarkActive(time.Time) (int64, error)
	ResetActive() (int64, error)
}
