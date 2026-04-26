.PHONY: lint lint_diff test test_v test_run test_cov clean doc build

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

build:
	go build -o bin/server ./cmd/server

docker-up:
	docker compose -f examples/docker/compose.yaml up -d
	docker compose -f examples/docker/compose.yaml logs

docker-down:
	docker compose -f examples/docker/compose.yaml down -v

docker-logs:
	docker compose -f examples/docker/compose.yaml logs

run-example-go-mysql-driver:
	cd examples/go-mysql-driver && go run main.go
