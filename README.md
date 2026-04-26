# MineSQL

MineSQL is a simple RDB inspired by MySQL.

## Documentation

See [docs](./docs) for more details. (Now only japanese)

## Usage

### Start Server

```sh
make build
./bin/server -h <host> -p <port> --init-user <username> --init-host <allowed-host>
```

| Argument | Description |
| -------- | ----------- |
| `-h` | Hostname or IP address |
| `-p` | Port number |
| `--init-user` | Initial username (first startup only) |
| `--init-host` | Allowed host pattern: `%` for all hosts, `192.168.1.%` for subnet |

A random password is generated and printed to the server log. On subsequent startups, `--init-user` and `--init-host` are not needed.

### Connect

MineSQL implements the MySQL protocol, so you can connect using the MySQL client. Connection always uses TLS.

```sh
mysql -u <username> -p<password> -h <host> -P <port> --default-auth=caching_sha2_password --ssl-mode=REQUIRED
```

To change the password:

```sql
ALTER USER '<username>'@'<host>' IDENTIFIED BY '<new-password>';
```

## Settings

| Environment Variable | Description | Default Value |
| -------------------- | ----------- | ------------- |
| `MINESQL_DATA_DIR` | Data file storage directory | `./data` |
| `MINESQL_BUFFER_SIZE` | Buffer pool size (number of pages) | `100` |
| `MINESQL_REDO_LOG_MAX_SIZE` | Max redo log size (bytes) for page cleaner trigger | `1048576` (1MB) |
| `MINESQL_MAX_DIRTY_PAGES_PCT` | Max dirty page percentage for page cleaner trigger | `90` |

## Examples

| Example | Description |
| ------- | ----------- |
| [go-mysql-driver](./examples/go-mysql-driver) | Connect to MineSQL using go-sql-driver/mysql |

## Feature

| Statement | Implementation |
| --------- | -------------- |
| [CREATE TABLE](./docs/feature/create-table.md) | ✅ |
| [SELECT](./docs/feature/select.md) | ✅ |
| [INSERT](./docs/feature/insert.md) | ✅ |
| [DELETE](./docs/feature/delete.md) | ✅ |
| [UPDATE](./docs/feature/update.md) | ✅ |
| [Transaction](./docs//feature/transaction.md) | ✅ |
| [ALTER USER](./docs/feature/alter-user.md) | ✅ |
| [Account](./docs/feature/account.md) | ✅ |
