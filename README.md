# hawk-synthetic-monitor

Synthetic monitoring for [Hawk](https://hawk.so): on a fixed interval it sends a test event and checks that it makes it through the whole pipeline — collector, processing, project view, notification.

Test assignment for the CodeX club (Developer Infrastructure → Synthetic Monitoring). Work in progress.

## Development

```
make check   # format check, lint, tests
make fmt     # apply formatters (gofumpt, gci)
make hooks   # enable the pre-commit hook
```

## Requirements

A running Hawk: collector, API and the `default`, `grouper`, `notifier` and `webhook` workers.
