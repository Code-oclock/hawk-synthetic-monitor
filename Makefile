.PHONY: fmt fmt-check lint test check build hooks

fmt:
	golangci-lint fmt

fmt-check:
	golangci-lint fmt --diff

lint:
	golangci-lint run

test:
	go test -race -count=1 ./...

check: fmt-check lint test

build:
	go build -o bin/monitor ./cmd/monitor

hooks:
	git config core.hooksPath .githooks
