DIFF_FILE := "$$(git diff --name-only --diff-filter=ACMRT | grep .go$ | xargs -I{} dirname {} | sort | uniq | xargs -I{} echo ./{})"

.PHONY: lint lint_diff test test_cov clean doc build build-client build-server

lint:
	golangci-lint run
lint_diff:
	golangci-lint run $$(echo $(DIFF_FILE))

test:
	go test -v ./internal/...
test_cov:
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
