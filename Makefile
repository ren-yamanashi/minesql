.PHONY: lint lint_diff test test_v test_run test_cov clean doc build build-client build-server

lint:
	golangci-lint run

test:
	go test ./internal/...

test_v:
	go test -v ./internal/...

test_run:
	go test -v -run $(RUN) ./internal/...

test_cov:
	go test -cover ./internal/...

clean:
	go clean -testcache
	rm -rf bin/

doc:
	@echo "Starting godoc server at http://localhost:6060 (e.g. http://localhost:6060/pkg/minesql/internal/storage/disk/)"
	@echo "Press Ctrl+C to stop"
	godoc -http=:6060

build-client:
	go build -o bin/client ./cmd/client

build-server:
	go build -o bin/server ./cmd/server

build: build-client build-server
