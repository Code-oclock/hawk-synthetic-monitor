package hawk

import (
	"errors"
	"fmt"
)

// ErrProtocol indicates a protocol violation from the Hawk service.
var ErrProtocol = errors.New("hawk: protocol error")

// CollectorError is returned when the collector rejects an event.
type CollectorError struct {
	Code    int
	Message string
}

func (e *CollectorError) Error() string {
	return fmt.Sprintf("collector rejected event: %d %s", e.Code, e.Message)
}
