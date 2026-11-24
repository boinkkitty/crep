// Package idgen generates time-ordered UUIDv7 identifiers.
package idgen

import "github.com/google/uuid"

type UUIDv7 struct{}

func (UUIDv7) NewID() string { return uuid.Must(uuid.NewV7()).String() }
