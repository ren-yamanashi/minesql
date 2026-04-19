# 非ユニークインデックスの動作確認

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

## ケース 1: 非ユニークインデックスの作成と同一キーの複数行挿入

### 手順

```sql
CREATE TABLE products (id VARCHAR, name VARCHAR, category VARCHAR, PRIMARY KEY (id), KEY idx_category (category));
BEGIN;
INSERT INTO products (id, name, category) VALUES ('1', 'Apple', 'Fruit'), ('2', 'Banana', 'Fruit'), ('3', 'Carrot', 'Veggie');
COMMIT;
SELECT * FROM products WHERE category = 'Fruit';
```

### 期待結果

- INSERT でユニーク制約エラーにならない (同一カテゴリ "Fruit" で複数行)
- SELECT で 2 行が返される (id=1 Apple, id=2 Banana)

## ケース 2: 非ユニークインデックスを使った JOIN

### 手順

```sql
CREATE TABLE users (id VARCHAR, name VARCHAR, PRIMARY KEY (id));
CREATE TABLE orders (id VARCHAR, user_id VARCHAR, item VARCHAR, PRIMARY KEY (id), KEY idx_user_id (user_id));
BEGIN;
INSERT INTO users (id, name) VALUES ('1', 'Alice'), ('2', 'Bob'), ('3', 'Charlie');
INSERT INTO orders (id, user_id, item) VALUES ('100', '1', 'apple'), ('101', '1', 'banana'), ('102', '3', 'cherry');
COMMIT;
SELECT * FROM users JOIN orders ON users.id = orders.user_id;
```

### 期待結果

- 3 行が返される
  - users.id=1 (Alice) × orders (apple, banana) → 2 行
  - users.id=3 (Charlie) × orders (cherry) → 1 行
- users.id=2 (Bob) は orders に対応する行がないため結果に含まれない

## ケース 3: UNIQUE KEY と KEY の混在

### 手順

```sql
CREATE TABLE items (id VARCHAR, name VARCHAR, category VARCHAR, PRIMARY KEY (id), UNIQUE KEY idx_name (name), KEY idx_category (category));
BEGIN;
INSERT INTO items (id, name, category) VALUES ('1', 'Apple', 'Fruit'), ('2', 'Banana', 'Fruit'), ('3', 'Carrot', 'Veggie');
COMMIT;
SELECT * FROM items WHERE category = 'Fruit';
SELECT * FROM items WHERE name = 'Banana';
```

### 期待結果

- `category = 'Fruit'`: 2 行が返される (非ユニークインデックスで検索)
- `name = 'Banana'`: 1 行が返される (ユニークインデックスで検索)
