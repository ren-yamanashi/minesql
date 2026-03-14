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

## Feature

| Statement | Implementation |
| --------- | -------------- |
| [CREATE TABLE](./docs/feature/create-table.md) | ✅ |
| [SELECT](./docs/feature/select.md) | ✅ |
| [INSERT](./docs/feature/insert.md) | ✅ |
| [DELETE](./docs/feature/delete.md) | ✅ |
| [UPDATE](./docs/feature/update.md) | ✅ |
| JOIN | - |
| Transaction | - |
