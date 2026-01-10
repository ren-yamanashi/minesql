.PHONY: test test-cov clean

test:
	go test -v ./internal/...

test-cov:
	go test -v -cover ./internal/...

clean:
	go clean -testcache
