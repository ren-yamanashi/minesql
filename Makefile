.PHONY: fmt test test-cov clean doc

fmt:
	find . -name "*.go" -type f -exec goimports -w {} \;

test:
	go test -v ./internal/...

test-cov:
	go test -v -cover ./internal/...

clean:
	go clean -testcache

doc:
	@echo "Starting godoc server at http://localhost:6060 (e.g. http://localhost:6060/pkg/minesql/internal/storage/disk/)"
	@echo "Press Ctrl+C to stop"
	godoc -http=:6060
