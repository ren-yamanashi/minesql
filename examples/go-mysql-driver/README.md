# go-sql-driver/mysql Example

go-sql-driver/mysql を使用して MineSQL に接続するサンプル

## 前提条件

```sh
# pwd -> ~/path/to/minesql
make build
rm -rf data
./bin/server -h localhost -p 18888 --init-user root --init-host %
# => Initial user 'root'@'%' created with password: <generated-password>
```

## Run

```sh
# pwd -> ~/path/to/minesql
MINESQL_PASSWORD=<generated-password> make run-example-go-mysql-driver
```

## Output

```text
Connected to MineSQL

--- CREATE TABLE ---
Created table: users

--- INSERT ---
Inserted 3 rows

--- SELECT * FROM users ---
  id=1, name=Alice
  id=2, name=Bob
  id=3, name=Charlie

--- UPDATE ---
Updated 1 rows
  id=1, name=Alice Updated

--- DELETE ---
Deleted 1 rows

--- Transaction: COMMIT ---
  INSERT id=4 (in transaction)
  Committed

--- Transaction: ROLLBACK ---
  DELETE id=1 (in transaction)
  Rolled back - id=1 should still exist

=== Final Result ===
--- SELECT * FROM users ---
  id=1, name=Alice Updated
  id=2, name=Bob
  id=4, name=Dave
```
