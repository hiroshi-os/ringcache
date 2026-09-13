.PHONY: build test race bench cluster stop demo fmt

build:
	mkdir -p bin
	go build -o bin/ringcache ./cmd/ringcache
	go build -o bin/ringcache-bench ./cmd/bench

fmt:
	gofmt -w cmd internal

test:
	go test ./... -count=1

race:
	go test ./... -race -count=1

cluster: build
	./scripts/run-cluster.sh

stop:
	./scripts/stop-cluster.sh

bench: build
	./scripts/bench.sh

demo: build
	./scripts/failure-demo.sh
