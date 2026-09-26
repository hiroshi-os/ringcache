.PHONY: build test race bench cluster stop demo rebalance quickstart fmt ci ratebench

build:
	mkdir -p bin
	go build -o bin/ringcache ./cmd/ringcache
	go build -o bin/ringcache-bench ./cmd/bench
	go build -o bin/ratebench ./cmd/ratebench
	go build -o bin/ratelimit-demo ./examples/ratelimit-demo

fmt:
	gofmt -w cmd internal pkg examples

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

ratebench: build
	./scripts/ratebench.sh

quickstart:
	./scripts/quickstart.sh

demo: build
	./scripts/failure-demo.sh

rebalance: build
	./scripts/rebalance-demo.sh

ci:
	test -z "$$(gofmt -l cmd internal pkg examples)"
	go vet ./...
	go test ./... -race -count=1
