package database

import "github.com/lib/pq"

type pqError struct {
	err *pq.Error
}

func (e *pqError) Error() string {
	return e.err.Error()
}

func (e *pqError) Conflict() bool {
	return e.err.Code.Class() == "23"
}

type errConflict interface {
	Conflict() bool
}

func IsConflictError(err error) bool {
	e, ok := err.(errConflict)
	return ok && e.Conflict()
}
