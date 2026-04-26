# 外部キーの動作確認

## 前提

- サーバーとクライアントをビルド済みであること

## 準備

```sh
make build
```

サーバーを起動:

```sh
./bin/server -h localhost -p 18888
```

## ケース 1: FK 付きテーブルの作成と正常な INSERT

### 手順

```sql
CREATE TABLE users (id VARCHAR, name VARCHAR, PRIMARY KEY (id));
CREATE TABLE orders (id VARCHAR, user_id VARCHAR, PRIMARY KEY (id), KEY idx_user_id (user_id), FOREIGN KEY fk_user (user_id) REFERENCES users (id));
BEGIN;
INSERT INTO users (id, name) VALUES ('1', 'Alice'), ('2', 'Bob');
INSERT INTO orders (id, user_id) VALUES ('100', '1'), ('101', '2');
COMMIT;
SELECT * FROM orders;
```

### 期待結果

- INSERT がエラーにならない (参照先の users に値が存在するため)
- 同一トランザクション内で親を INSERT した直後に子から参照できる
- SELECT で 2 行が返される (id=100, id=101)

## ケース 2: 参照先に存在しない値で INSERT するとエラー

### 手順

```sql
BEGIN;
INSERT INTO orders (id, user_id) VALUES ('102', '999');
ROLLBACK;
```

### 期待結果

- INSERT が FK 制約違反のエラーになる (users に id=999 が存在しないため)

## ケース 3: 参照されている親レコードは削除できない

### 手順

```sql
BEGIN;
DELETE FROM users WHERE id = '1';
ROLLBACK;
```

### 期待結果

- DELETE が FK 制約違反のエラーになる (orders から users.id=1 が参照されているため)

## ケース 4: FK カラムを存在しない値に UPDATE するとエラー

### 手順

```sql
BEGIN;
UPDATE orders SET user_id = '999' WHERE id = '100';
ROLLBACK;
```

### 期待結果

- UPDATE が FK 制約違反のエラーになる (users に id=999 が存在しないため)

## ケース 5: 参照されている親の PK を UPDATE するとエラー

### 手順

```sql
BEGIN;
UPDATE users SET id = '999' WHERE id = '2';
ROLLBACK;
```

### 期待結果

- UPDATE が FK 制約違反のエラーになる (orders から users.id=2 が参照されているため)

## ケース 6: 子を削除してから親を削除すれば成功する

### 手順

```sql
BEGIN;
DELETE FROM orders WHERE user_id = '1';
DELETE FROM users WHERE id = '1';
COMMIT;
SELECT * FROM users;
SELECT * FROM orders;
```

### 期待結果

- 両方の DELETE がエラーにならない
- users に id=1 の行が存在しない
- orders に user_id=1 の行が存在しない
