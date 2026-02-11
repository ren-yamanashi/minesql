# minesql

## Usage

### Start Server

```sh
make build-server
./bin/server -h localhost -p 8888 # -h = hostname, -p = port
```

### Start Client

```sh
make build-client
./bin/client -h localhost -p 8888 # -h = hostname, -p = port
```

## Implementation

- [ ] CLI Client
- [ ] TCP Server
- [x] Parser
- [x] Planner (Not optimized)
- [x] Executor
- [x] Storage Engine

## Feature

| Statement | Implementation | Note |
| --------- | -------------- | ---- |
| [CREATE TABLE](./docs/feature/create-table.md) | ✅ | - |
| [INSERT](./docs/feature/insert.md) | ✅ | - |
| [SELECT](./docs/feature/select.md) | ✅ | - |
| UPDATE | - | - |
| DELETE | - | - |
| JOIN | - | - |
| Transaction | - | - |

## Development

### Build

- output: `bin/`

```sh
make build

# only client
make build-client

# only server
make build-server
```

### Test

```sh
make test # or make test-cov
```

### Format

```sh
make fmt
```

### Clean up

```sh
make clean
```

### Documentation

```sh
make doc
```
