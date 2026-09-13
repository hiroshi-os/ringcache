.PHONY: build test race bench cluster stop demo rebalance quickstart fmt ci

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

quickstart:
	./scripts/quickstart.sh

demo: build
	./scripts/failure-demo.sh

rebalance: build
	./scripts/rebalance-demo.sh

ci:
	test -z "$$(gofmt -l cmd internal)"
	go vet ./...
	go test ./... -race -count=1
