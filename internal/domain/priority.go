package domain

import "strings"

// Priority orders claims; lower values are claimed first.
type Priority int

const (
	PriorityCritical Priority = 0
	PriorityHigh     Priority = 1
	PriorityNormal   Priority = 2
	PriorityLow      Priority = 3
)

var priorityNames = [...]string{"critical", "high", "normal", "low"}

func (p Priority) String() string {
	if p.Valid() {
		return priorityNames[p]
	}
	return "unknown"
}

// Valid reports whether p is one of the four defined levels.
func (p Priority) Valid() bool { return p >= PriorityCritical && p <= PriorityLow }

// ParsePriority parses a priority name. An empty string yields normal.
func ParsePriority(s string) (Priority, error) {
	if s == "" {
		return PriorityNormal, nil
	}
	for i, n := range priorityNames {
		if strings.EqualFold(s, n) {
			return Priority(i), nil
		}
	}
	return 0, invalid("priority", "must be one of critical, high, normal, low")
}
