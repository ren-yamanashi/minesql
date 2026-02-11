.PHONY: fmt test test-cov clean doc build build-client build-server

fmt:
	find . -name "*.go" -type f -exec goimports -w {} \;

test:
	go test -v ./internal/...

test-cov:
	go test -v -cover ./internal/...

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
