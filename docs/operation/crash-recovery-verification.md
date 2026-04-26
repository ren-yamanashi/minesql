# クラッシュリカバリの動作確認

## 前提

- サーバーを正常終了 (Ctrl+C) すると、全ダーティーページがフラッシュされ REDO ログがクリアされる
- クラッシュをシミュレーションするには、サーバープロセスを強制終了 (`kill -9`) する

## 準備

```sh
make build
```

## ケース 1: コミット済み変更の復元

コミット済みの変更がクラッシュ後に失われないことを確認する。

### 手順

ターミナル 1 (サーバー):

```sh
./bin/server -h localhost -p 8888
```

ターミナル 2 (クライアント):

```sh
mysql -u root -proot -h 127.0.0.1 -P 8888 --default-auth=caching_sha2_password
```

```sql
CREATE TABLE users (id VARCHAR, name VARCHAR, PRIMARY KEY (id));
BEGIN;
INSERT INTO users (id, name) VALUES ('1', 'Alice');
COMMIT;
```

ターミナル 3 (強制終了):

```sh
kill -9 $(pgrep -f './bin/server')
```

ターミナル 1 (サーバー再起動):

```sh
./bin/server -h localhost -p 8888
```

ターミナル 2 (クライアント再接続):

```sh
mysql -u root -proot -h 127.0.0.1 -P 8888 --default-auth=caching_sha2_password
```

```sql
BEGIN;
SELECT * FROM users;
COMMIT;
```

### 期待結果

`SELECT` で `1, Alice` が返る。REDO ログから復元されている。

## ケース 2: 未コミット変更のロールバック

コミットしていない変更がクラッシュ後に取り消されることを確認する。

### 手順

テーブルが事前にディスクに永続化されている必要がある (DDL は REDO ログに記録されないため)。ケース 1 の後に続けて実行するか、以下の手順でテーブルを作成して正常終了しておく。

ターミナル 1 (サーバー):

```sh
./bin/server -h localhost -p 8888
```

ターミナル 2 (クライアント) - テーブル作成後、Ctrl+C でサーバーを正常終了し再起動:

```sh
mysql -u root -proot -h 127.0.0.1 -P 8888 --default-auth=caching_sha2_password
```

```sql
CREATE TABLE users (id VARCHAR, name VARCHAR, PRIMARY KEY (id));
```

サーバーを Ctrl+C で正常終了して再起動する。

```sh
./bin/server -h localhost -p 8888
```

ターミナル 2 (クライアント再接続) - INSERT (COMMIT なし):

```sh
mysql -u root -proot -h 127.0.0.1 -P 8888 --default-auth=caching_sha2_password
```

```sql
BEGIN;
INSERT INTO users (id, name) VALUES ('1', 'Alice');
```

**COMMIT しない**。

ターミナル 3 (強制終了):

```sh
kill -9 $(pgrep -f './bin/server')
```

ターミナル 1 (サーバー再起動):

```sh
./bin/server -h localhost -p 8888
```

ターミナル 2 (クライアント再接続):

```sh
mysql -u root -proot -h 127.0.0.1 -P 8888 --default-auth=caching_sha2_password
```

```sql
BEGIN;
SELECT * FROM users;
COMMIT;
```

### 期待結果

`SELECT` で空の結果が返る。未コミットの INSERT が UNDO ログによりロールバックされている。

## ケース 3: 正常終了後のリカバリ不要

正常終了後の再起動ではリカバリが実行されないことを確認する。

### 手順

```sh
./bin/server -h localhost -p 8888
```

```sh
mysql -u root -proot -h 127.0.0.1 -P 8888 --default-auth=caching_sha2_password
```

```sql
CREATE TABLE users (id VARCHAR, name VARCHAR, PRIMARY KEY (id));
BEGIN;
INSERT INTO users (id, name) VALUES ('1', 'Alice');
COMMIT;
```

Ctrl+C でサーバーを正常終了し、再起動する。

```sh
./bin/server -h localhost -p 8888
```

### 期待結果

サーバーが即座に起動する (リカバリのログが出力されない)。データは正常に存在する。
