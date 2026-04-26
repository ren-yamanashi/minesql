FROM golang:1.25-alpine AS builder

RUN go install github.com/ren-yamanashi/minesql/cmd/server@latest

FROM debian:bookworm-slim

WORKDIR /app
COPY --from=builder /go/bin/server ./server

EXPOSE 3306

ENTRYPOINT ["./server"]
CMD ["-h", "0.0.0.0", "-p", "3306", "--init-user", "root", "--init-host", "%"]
