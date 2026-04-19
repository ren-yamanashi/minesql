# エグゼキュータ

## 概要

- エグゼキュータは、プランナーに指定された通りにクエリを実行する
  - それぞれのエグゼキュータは、「範囲検索をするもの」「カラム値を利用して検索するもの」「レコードを追加するもの」など役割ごとに分かれている
  - それらの種類のエグゼキュータを組み合わせて、一つの大きなエグゼキュータを構成する
  - 組み合わさったエグゼキュータは、ツリー構造をしている
- エグゼキュータでクエリを実行する前に、UNDO ログに必要な情報を記録する
  - クエリ実行前に UNDO ログの記録を行うのは、クエリ実行中にエラーが発生したときに、UNDO ログを元にロールバックできるようにするため

## エグゼキュータのツリー構造

例として

```sql
CREATE TABLE users (
 id VARCHAR,
 first_name VARCHAR,
 last_name VARCHAR,
 gender VARCHAR,
 username VARCHAR,
 PRIMARY KEY (id),
 UNIQUE KEY username_UNIQUE (username)
);
```

というテーブルに対してクエリを投げたときを考える

### 前提

- ツリーを構成するノードの種類は以下の通り
  - TableScan: テーブルをプライマリキーで範囲検索する
  - IndexScan: セカンダリインデックスを利用して検索する (ユニーク・非ユニーク両対応)
  - NestedLoopJoin: 左の各行に対して右のテーブルを検索し、結合する
  - Filter: 不要な行をフィルタする
  - Union: 複数のスキャン結果を結合し、重複を除去する
  - CreateTable: テーブルを作成する
  - Project: 検索結果から特定のカラムだけを取り出す
  - Insert: レコードを追加する
  - Update: レコードを更新する
  - Delete: レコードを削除する

※それぞれのノードの命名はオレオレだが、少し https://dev.mysql.com/doc/dev/mysql-server/latest/classRowIterator.html を参考にしている

### 例1

```sql
-- id は PRIMARY KEY
SELECT first_name, last_name FROM users WHERE id = 1;
```

PRIMARY KEY なので、TableScan が B+Tree のキー検索で直接条件を処理でき、Filter を経由しない。

```txt
Project (first_name, last_name)
  └── TableScan (id = 1)
```

### 例2

```sql
-- gender にインデックスなし
SELECT * FROM users WHERE gender = 'male';
```

インデックスがないため、TableScan はフルスキャンしか行えず、フィルタリングは上位の Filter ノードが担当する。

```txt
Project (*)
  └── Filter (gender = 'male')
        └── TableScan (フルスキャン)
```

### 例3

```sql
-- username に UNIQUE INDEX がある
SELECT * FROM users WHERE username = 'alice';
```

UNIQUE INDEX があるため IndexScan を利用して検索できる。SELECT * なので全カラムが必要となり、IndexScan はインデックスからマッチするエントリを見つけた後、そこに含まれる PK でテーブル本体を検索して全カラムを取得する。

```txt
Project (*)
  └── IndexScan (username = 'alice')
```

### 例4

```sql
-- id は PRIMARY KEY、username に UNIQUE INDEX がある
SELECT * FROM users WHERE id = 1 OR username = 'alice';
```

それぞれにインデックスがあるため、各スキャン結果を Union で結合できる。

- TableScan: PK を利用して `id = 1` を検索
- IndexScan: セカンダリインデックスを利用して `username = 'alice'` を検索
- Union: 両方の結果を結合し、重複を除去
- Project: 検索結果から全カラムを取り出す

```txt
Project (*)
  └── Union
        ├── TableScan (id = 1)
        └── IndexScan (username = 'alice')
```

### 例5

```sql
-- orders.user_id に UNIQUE INDEX がある
-- users が orders より行数が少ない
SELECT * FROM users
INNER JOIN orders ON users.id = orders.user_id
WHERE orders.item = 'apple';
```

users が駆動表に選ばれ、orders は UNIQUE INDEX で eq_ref アクセスする\
WHERE 条件 (`orders.item = 'apple'`) は内部表のカラムなので駆動表側に分離できず、結合後に Filter で適用される (詳細: [プランナー - WHERE 条件の分離](../planner/planner.md#where-条件の分離))。

```txt
Project (*)
  └── Filter (orders.item = 'apple')
        └── NestedLoopJoin
              ├── TableScan (users: フルスキャン)
              └── IndexScan (orders: user_id UNIQUE INDEX で eq_ref)
```

### 例6

```sql
-- orders.user_id に UNIQUE INDEX がある
-- users が orders より行数が少ない
SELECT * FROM users
INNER JOIN orders ON users.id = orders.user_id;
```

SELECT * なので全カラムが必要となり、内部表 (orders) の IndexScan はインデックスからマッチするエントリを見つけた後、PK でテーブル本体を検索して全カラムを取得する。

```txt
Project (*)
  └── NestedLoopJoin
        ├── TableScan (users: フルスキャン)
        └── IndexScan (orders: user_id UNIQUE INDEX で eq_ref)
```

### 例7

```sql
-- id は PRIMARY KEY、username に UNIQUE INDEX がある
SELECT id, username FROM users WHERE username = 'alice';
```

SELECT カラム (id, username) は PK + UNIQUE INDEX カラムだけで構成されるため、index-only scan が適用される。インデックスのリーフにはセカンダリキーと PK の値が格納されているので、テーブル本体を読まずにインデックスだけで結果を返せる。

```txt
Project (id, username)
  └── IndexScan (username = 'alice', index-only)
```

- IndexScan は内部モード (`indexOnly`) で index-only scan を行う。独立したノード型ではなく、IndexScan のフラグで制御する
- SELECT カラムがインデックスでカバーされないカラムを含む場合は通常の IndexScan (PK でテーブル本体を検索) になる

### ツリー図

全ての Executor は共通の `Executor` interface (`Next() (Record, error)`) を実装する。
一部の Executor は `InnerExecutor` を持ち、子ノードから `Next()` でデータを受け取って処理する。

```txt
Executor (interface)
  │
  ├── TableScan          (リーフノード: テーブル全体を走査する)
  ├── IndexScan          (リーフノード: セカンダリインデックスを利用して検索する)
  │
  ├── NestedLoopJoin     (ブランチノード: 左の各行に対して右の Executor を生成し、結合する)
  │     └── InnerExecutor
  ├── Filter             (ブランチノード: InnerExecutor の結果から条件に合う行だけを返す)
  │     └── InnerExecutor
  ├── Union              (ブランチノード: 複数の InnerExecutor の結果を結合し、重複を除去する)
  │     ├── InnerExecutor1
  │     ├── InnerExecutor2
  │     └── ...
  ├── Project            (ブランチノード: InnerExecutor の結果から特定のカラムだけを取り出す)
  │     └── InnerExecutor
  │
  ├── Delete             (ブランチノード: InnerExecutor の結果を元にレコードを削除する)
  │     └── InnerExecutor
  ├── Update             (ブランチノード: InnerExecutor の結果を元にレコードを更新する)
  │     └── InnerExecutor
  │
  ├── Insert             (リーフノード: レコードを追加する)
  └── CreateTable        (リーフノード: テーブルを作成する)
```

- リーフノード: 子を持たず、自身でストレージにアクセスしてデータを取得・操作する
- ブランチノード: `InnerExecutor` を通じて子ノードからデータを受け取り、加工・操作する
