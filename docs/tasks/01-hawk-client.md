# Задача 1: клиент Hawk (`internal/hawk`)

**Цель.** Научиться отправлять тестовое событие в collector Hawk и находить его через GraphQL API. Это ядро всех будущих проверок: сценарии мониторинга будут вызывать эти методы и замерять время.

**Как работать.** Тесты уже написаны (`internal/hawk/client_test.go`) и сейчас падают — методы-заглушки паникуют. Твоя работа: заменить заглушки в `internal/hawk/client.go` реализацией, пока `go test -race ./...` не станет зелёным. Тесты не правь; если считаешь, что тест неверен, — напиши мне, обсудим.

## Что нужно реализовать
`New`, `SendEvent`, `FindEvent`, `WaitForEvent` (сигнатуры и комментарии — в `client.go`).

### SendEvent
`POST` на `CollectorURL` с JSON:
```json
{
  "token": "<IntegrationToken>",
  "catcherType": "errors/golang",
  "payload": {
    "title": "<title>",
    "type": "error",
    "backtrace": [{"file": "main.go", "line": 1, "function": "main"}]
  }
}
```
Ответ collector всегда JSON: `{"code":200,"error":false,"message":"OK"}`. При отказе то же, но `error:true`, `code` = 400/402/413/429 и текст в `message` (например `Integration token invalid: …`, `Token decoding error`, `Project has exceeded the events limit`). Любой отказ → `*CollectorError`. Ошибки сети и отмену контекста возвращай как есть (с оборачиванием `%w`), чтобы работало `errors.Is(err, context.DeadlineExceeded)`.

### FindEvent
1. Если токена ещё нет — войти (один раз, дальше переиспользовать; клиент может вызываться из нескольких горутин):
   ```graphql
   mutation($e: String!, $p: String!) { login(email: $e, password: $p) { accessToken } }
   ```
2. Запросить последние события проекта (заголовок `Authorization: Bearer <accessToken>`):
   ```graphql
   query($id: ID!) { project(projectId: $id) { events(limit: 20) { id totalCount payload { title } } } }
   ```
   Ответ: `{"data":{"project":{"events":[{"id":"…","totalCount":1,"payload":{"title":"…"}}]}}}`.
3. Вернуть событие, у которого `title` совпадает **точно** (не `Contains`: `run-2` и `run-22` — разные события). Нет такого — `ErrNotFound`.

GraphQL при ошибке отвечает HTTP 200 и `{"errors":[{"message":"…"}],"data":null}` — проверять надо поле `errors`, а не только статус. Текст ошибки включи в возвращаемую `error`.

### WaitForEvent
Вызывает `FindEvent` с интервалом, пока не найдёт или не истечёт `ctx`. Событие может появиться не сразу (его обрабатывают воркеры), поэтому `ErrNotFound` — повод подождать. Любая другая ошибка (неверный пароль, сеть) — повод вернуться сразу: иначе мониторинг будет «ждать» до таймаута вместо того, чтобы назвать настоящую причину.

## Подсказки
- `http.NewRequestWithContext` — тогда отмена контекста прервёт запрос.
- У `http.Client` из коробки нет таймаута — задай его (см. поле `Config.HTTPClient`).
- `time.NewTicker` + `select` на `ctx.Done()` для опроса.
- Токен храни под `sync.Mutex`; `-race` в тестах это проверит.
- Не пиши `panic`/`log.Fatal` в библиотечном коде — возвращай ошибки.

## Проверка на живом Hawk (когда тесты зелёные)
1. Поднять стенд (`~/work/codex-intake/hawk-local/local/README.md`) и выполнить `bootstrap.sh` — он создаст `local/stand.env` с токеном и project id.
2. Написать в `cmd/monitor/main.go` короткий запуск: отправить событие с уникальным title, дождаться его через `WaitForEvent` с шагом 250 мс и таймаутом 10 с, вывести время каждого этапа. Ожидание: отправка ≈ 20 мс, событие видно меньше чем через секунду.
3. Проверить отказ: испортить токен — `SendEvent` должен вернуть `*CollectorError` с понятным текстом.

## Готово, когда
- `go test -race ./...` зелёный, `go vet ./...` и `gofmt -l .` чистые;
- живой прогон из п. 2 показывает время этапов;
- пришли мне код на ревью — разберём названия, обработку ошибок и что можно упростить.
