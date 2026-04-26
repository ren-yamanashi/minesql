# トランザクションの動作確認

## 前提

- サーバーとクライアントをビルド済みであること
- 2 つのクライアントターミナルを使用する

## 準備

```sh
make build
```

サーバーを起動:

```sh
./bin/server -h localhost -p 8888
```

## ケース 1: 未コミットの INSERT は他セッションから見えない

### 手順

クライアント A:

```sql
CREATE TABLE users (id VARCHAR, name VARCHAR, PRIMARY KEY (id));
BEGIN;
INSERT INTO users (id, name) VALUES ('1', 'Alice');
-- ここで COMMIT しない
```

クライアント B:

```sql
BEGIN;
SELECT * FROM users;
-- 結果: 0 件 (A の INSERT は未コミットなので見えない)
COMMIT;
```

クライアント A:

```sql
COMMIT;
```

### 期待結果

- クライアント B の SELECT はクライアント A の未コミット INSERT を見ない
- クライアント A が COMMIT した後に再度 SELECT すれば見える

## ケース 2: コミット済みの変更は新しいトランザクションから見える

### 手順

クライアント A:

```sql
BEGIN;
INSERT INTO users (id, name) VALUES ('2', 'Bob');
COMMIT;
```

クライアント B:

```sql
BEGIN;
SELECT * FROM users;
-- 結果: ('1', 'Alice'), ('2', 'Bob')
COMMIT;
```

### 期待結果

- A がコミットした INSERT は B の新しいトランザクションから見える

## ケース 3: UPDATE の排他ロック競合

### 手順

クライアント A:

```sql
BEGIN;
UPDATE users SET name = 'Carol' WHERE id = '1';
-- ここで COMMIT しない (排他ロックを保持中)
```

クライアント B:

```sql
BEGIN;
UPDATE users SET name = 'Dave' WHERE id = '1';
-- A が排他ロックを保持しているためタイムアウト待ち
-- タイムアウトするとエラーが返る
```

クライアント A:

```sql
COMMIT;
```

### 期待結果

- B の UPDATE は A がロックを保持している間はブロックされ、タイムアウトする
- A が COMMIT するとロックが解放される

## ケース 4: ROLLBACK で変更が取り消される

### 手順

クライアント A:

```sql
BEGIN;
INSERT INTO users (id, name) VALUES ('3', 'Eve');
SELECT * FROM users;
-- 結果: 自分の INSERT は見える
ROLLBACK;
```

クライアント B:

```sql
BEGIN;
SELECT * FROM users;
-- 結果: id=3 の行は存在しない (A が ROLLBACK したため)
COMMIT;
```

### 期待結果

- ROLLBACK により INSERT が取り消され、他セッションからも見えない

## ケース 5: DELETE の動作確認

### 手順

クライアント A:

```sql
BEGIN;
DELETE FROM users WHERE id = '2';
COMMIT;
```

クライアント B:

```sql
BEGIN;
SELECT * FROM users;
-- 結果: id=2 の行が消えている
COMMIT;
```

### 期待結果

- コミット済みの DELETE は新しいトランザクションに反映される

## ケース 6: Fuzzy Read (Non-Repeatable Read) が発生しない

同一トランザクション内で同じ行を 2 回読んだとき、間に他セッションの UPDATE が入っても結果が変わらないことを確認する。

### 手順

クライアント A:

```sql
BEGIN;
SELECT * FROM users WHERE id = '1';
-- 結果: ('1', 'Alice')
-- ここで一旦止める (COMMIT しない)
```

クライアント B:

```sql
BEGIN;
UPDATE users SET name = 'Zara' WHERE id = '1';
COMMIT;
```

クライアント A (続き):

```sql
SELECT * FROM users WHERE id = '1';
-- 結果: ('1', 'Alice') のまま (B の UPDATE は見えない)
COMMIT;
```

### 期待結果

- A の 2 回目の SELECT でも B の UPDATE 前の値 `Alice` が返る
- REPEATABLE READ の Read View は最初の読み取り時に作成され、トランザクション終了まで使い回されるため、途中で他セッションがコミットした変更は見えない

## ケース 7: Phantom Read が発生しない

同一トランザクション内で同じ条件の SELECT を 2 回実行したとき、間に他セッションの INSERT が入っても結果件数が変わらないことを確認する。

### 手順

クライアント A:

```sql
BEGIN;
SELECT * FROM users;
-- 結果: 既存の行のみ (例: 1 件)
-- ここで一旦止める (COMMIT しない)
```

クライアント B:

```sql
BEGIN;
INSERT INTO users (id, name) VALUES ('99', 'Phantom');
COMMIT;
```

クライアント A (続き):

```sql
SELECT * FROM users;
-- 結果: 先ほどと同じ件数 (B の INSERT は見えない)
COMMIT;
```

### 期待結果

- A の 2 回目の SELECT でも B が INSERT した行は見えない
- Read View が固定されているため、A のトランザクション中に他セッションが挿入した行はファントムとして出現しない
