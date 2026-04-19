# JOIN の動作確認

## 前提

- サーバーとクライアントをビルド済みであること

## 準備

```sh
make build-server
make build-client
```

サーバーを起動:

```sh
./bin/server -h localhost -p 18888
```

## ケース 1: 基本的な INNER JOIN

### 手順

```sql
CREATE TABLE users (id VARCHAR, name VARCHAR, PRIMARY KEY (id));
CREATE TABLE orders (id VARCHAR, user_id VARCHAR, item VARCHAR, PRIMARY KEY (id), UNIQUE KEY idx_user_id (user_id));
BEGIN;
INSERT INTO users (id, name) VALUES ('1', 'Alice'), ('2', 'Bob'), ('3', 'Charlie');
INSERT INTO orders (id, user_id, item) VALUES ('100', '1', 'apple'), ('101', '3', 'banana');
COMMIT;
BEGIN;
SELECT * FROM users JOIN orders ON users.id = orders.user_id;
COMMIT;
```

### 期待結果

- 2 行が返される (users.id=1 と orders.user_id=1、users.id=3 と orders.user_id=3 がマッチ)
- users.id=2 (Bob) は orders に対応する行がないため結果に含まれない (INNER JOIN)

## ケース 2: JOIN + WHERE による絞り込み

### 手順

```sql
BEGIN;
SELECT * FROM users JOIN orders ON users.id = orders.user_id WHERE orders.item = 'banana';
COMMIT;
```

### 期待結果

- 1 行が返される (users.id=3, name=Charlie, orders.id=101, user_id=3, item=banana)

## ケース 3: WHERE 条件の分離による駆動表の最適化

### 手順

```sql
BEGIN;
SELECT * FROM users JOIN orders ON users.id = orders.user_id WHERE users.id = '1';
COMMIT;
```

### 期待結果

- 1 行が返される (users.id=1 の行のみが駆動表から読まれ、orders と結合される)
- WHERE 条件 `users.id = '1'` は駆動表側に分離され、PK の等値検索で 1 行のみ読む
