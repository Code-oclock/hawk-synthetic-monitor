# hawk-synthetic-monitor

Автономный сервис синтетического мониторинга [Hawk](https://hawk.so): раз в заданный интервал отправляет тестовое событие и проверяет, что оно прошло весь путь — collector → обработка → видно в проекте → уведомление.

Тестовое задание клуба CodeX (Developer Infrastructure → Synthetic Monitoring).

> Статус: в разработке. README будет дополнен архитектурой, запуском и описанием проверок.

## Запуск тестов
```
go test -race ./...
```

## Для работы нужен
Работающий экземпляр Hawk: collector, API и воркеры `default`, `grouper`, `notifier`, `webhook`. Локальный стенд собирается из [hawk.mono](https://github.com/codex-team/hawk.mono) (описание будет добавлено).
