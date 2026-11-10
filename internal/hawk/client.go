package hawk

import (
	"context"
	"net/http"
	"time"
)

// Config — параметры подключения к Hawk. Значения приходят из переменных окружения (см. .env.example).
type Config struct {
	CollectorURL     string       // например http://localhost:3000/
	APIURL           string       // например http://localhost:4000/graphql
	IntegrationToken string       // токен проекта для отправки событий
	ProjectID        string       // id проекта, в котором ищем события
	Email, Password  string       // учётная запись для GraphQL API
	HTTPClient       *http.Client // необязательно; по умолчанию — клиент с таймаутом
}

// Client общается с collector и GraphQL API одного проекта Hawk.
type Client struct {
	// TODO(1): поля клиента (конфиг, http-клиент, запомненный accessToken с защитой от гонок)
}

// New создаёт клиента.
func New(cfg Config) *Client {
	panic("TODO: см. docs/tasks/01-hawk-client.md")
}

// SendEvent отправляет в collector тестовое событие с заданным title.
// Коды отказа collector возвращаются как *CollectorError.
func (c *Client) SendEvent(ctx context.Context, title string) error {
	panic("TODO")
}

// FindEvent один раз ищет среди последних событий проекта событие с точно таким title.
// Входит в API при первом вызове и запоминает токен. Нет события — ErrNotFound.
func (c *Client) FindEvent(ctx context.Context, title string) (*Event, error) {
	panic("TODO")
}

// WaitForEvent повторяет FindEvent с интервалом interval, пока событие не появится или ctx не истечёт.
// Ошибка, не связанная с «события ещё нет» (например, неверный пароль), возвращается сразу.
func (c *Client) WaitForEvent(ctx context.Context, title string, interval time.Duration) (*Event, error) {
	panic("TODO")
}
