# MineSQL

MineSQL is a simple RDB inspired by MySQL.

## Documentation

See [docs](./docs) for more details. (Sorry, now only japanese)

## Usage

### Start Server

```sh
make build
./bin/server -h localhost -p 8888 # -h = hostname, -p = port
```

### Connect

```sh
mysql -u root -proot -h 127.0.0.1 -P 8888 --default-auth=caching_sha2_password
```

## Settings

| Environment Variable | Description | Default Value |
| -------------------- | ----------- | ------------- |
| `MINESQL_DATA_DIR` | Data file storage directory | `./data` |
| `MINESQL_BUFFER_SIZE` | Buffer pool size (number of pages) | `100` |
| `MINESQL_REDO_LOG_MAX_SIZE` | Max redo log size (bytes) for page cleaner trigger | `1048576` (1MB) |
| `MINESQL_MAX_DIRTY_PAGES_PCT` | Max dirty page percentage for page cleaner trigger | `90` |

## Feature

| Statement | Implementation | Details |
| --------- | -------------- | ------- |
| [CREATE TABLE](./docs/feature/create-table.md) | ✅ | |
| [SELECT](./docs/feature/select.md) | ✅ | |
| [INSERT](./docs/feature/insert.md) | ✅ | |
| [DELETE](./docs/feature/delete.md) | ✅ | |
| [UPDATE](./docs/feature/update.md) | ✅ | |
| [Transaction](./docs//feature/transaction.md) | ✅ | |
