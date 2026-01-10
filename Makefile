.PHONY: fmt test test-cov clean

fmt:
	find . -name "*.go" -type f -exec goimports -w {} \;

test:
	go test -v ./internal/...

test-cov:
	go test -v -cover ./internal/...

clean:
	go clean -testcache
