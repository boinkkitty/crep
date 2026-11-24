package mysql

import (
	"encoding/base64"
	"strings"
	"time"

	"crep/internal/domain"
)

// Keyset cursors encode (timestamp, id) for stable descending pagination.

func encodeCursor(t time.Time, id string) string {
	return base64.RawURLEncoding.EncodeToString([]byte(t.UTC().Format(time.RFC3339Nano) + "|" + id))
}

func decodeCursor(c string) (time.Time, string, error) {
	raw, err := base64.RawURLEncoding.DecodeString(c)
	if err != nil {
		return time.Time{}, "", badCursor()
	}
	ts, id, ok := strings.Cut(string(raw), "|")
	if !ok {
		return time.Time{}, "", badCursor()
	}
	t, err := time.Parse(time.RFC3339Nano, ts)
	if err != nil {
		return time.Time{}, "", badCursor()
	}
	return t, id, nil
}

func badCursor() error { return &domain.ValidationError{Field: "cursor", Message: "invalid"} }
