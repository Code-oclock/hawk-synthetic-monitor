// Package hawk sends test events to a Hawk collector and finds them through the GraphQL API.
package hawk

import (
	"errors"
	"fmt"
)

// ErrNotFound means the project has no event with the requested title.
var ErrNotFound = errors.New("hawk: event not found")

type Event struct {
	ID         string
	Title      string
	TotalCount int
}

// CollectorError is returned when the collector rejects an event.
type CollectorError struct {
	Code    int
	Message string
}

func (e *CollectorError) Error() string {
	return fmt.Sprintf("collector rejected event: %d %s", e.Code, e.Message)
}
