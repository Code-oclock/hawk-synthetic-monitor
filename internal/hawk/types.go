// Package hawk — клиент к Hawk: отправка тестового события в collector
// и поиск этого события через GraphQL API.
package hawk

import (
	"errors"
	"fmt"
)

// ErrNotFound возвращается, если события с таким title нет среди последних событий проекта.
var ErrNotFound = errors.New("hawk: event not found")

// Event — событие проекта, как его отдаёт GraphQL API.
type Event struct {
	ID         string
	Title      string
	TotalCount int
}

// CollectorError — collector принял запрос, но отказал (неверный токен, лимит и т.п.).
type CollectorError struct {
	Code    int    // код из ответа collector (совпадает с HTTP-статусом)
	Message string // текст из поля message
}

func (e *CollectorError) Error() string {
	return fmt.Sprintf("collector rejected event: %d %s", e.Code, e.Message)
}
