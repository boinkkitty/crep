// Package domain holds Crep's business rules. It performs no I/O.
package domain

import (
	"errors"
	"fmt"
)

var (
	// ErrNotFound means the requested entity does not exist for the caller's tenant.
	ErrNotFound = errors.New("not found")
	// ErrConflict means the operation is not valid for the entity's current state.
	ErrConflict = errors.New("conflict")
	// ErrStaleLease means a result was written by an executor that no longer owns the job.
	ErrStaleLease = errors.New("stale lease")
	// ErrDuplicate means a task with the same idempotency key already exists for the tenant.
	ErrDuplicate = errors.New("duplicate")
	// ErrVersionMismatch means the client's expected version (If-Match) is not current.
	ErrVersionMismatch = errors.New("version mismatch")
)

// ValidationError describes invalid user input.
type ValidationError struct {
	Field   string
	Message string
}

func (e *ValidationError) Error() string {
	if e.Field == "" {
		return e.Message
	}
	return fmt.Sprintf("%s: %s", e.Field, e.Message)
}

func invalid(field, format string, args ...any) error {
	return &ValidationError{Field: field, Message: fmt.Sprintf(format, args...)}
}

// IsValidation reports whether err is a ValidationError.
func IsValidation(err error) bool {
	var v *ValidationError
	return errors.As(err, &v)
}
