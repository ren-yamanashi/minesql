# エグゼキュータ

## 概要

- エグゼキュータは、プランナーに指定された通りにクエリを実行する
- それぞれのエグゼキュータは、「範囲検索をするもの」「カラム値を利用して検索するもの」「レコードを追加するもの」など役割ごとに分かれている
- SELECT, INSERT などのステートメント実行時には、それらの種類のエグゼキュータを組み合わせて、一つの大きなエグゼキュータを構成する

## ツリー構造

ツリーを構成するノードの種類は以下の通り

| ノード名 | 説明 |
| --- | --- |
| TableScan | テーブルをプライマリキーで範囲検索する |
| IndexScan | セカンダリインデックスを利用して検索する |
| NestedLoopJoin | 左の各行に対して右のテーブルを検索し、結合する |
| Filter | 不要な行をフィルタする |
| Union | 複数のスキャン結果を結合し、重複を除去する |
| CreateTable | テーブルを作成する |
| Project | 検索結果から特定のカラムだけを取り出す |
| Insert | レコードを追加する |
| Update | レコードを更新する |
| Delete | レコードを削除する |

※それぞれのノードの命名は少し https://dev.mysql.com/doc/dev/mysql-server/latest/classRowIterator.html を参考にしている

<br />

ツリーの内容は実行するステートメントやその内容によって変わるため、以下のテーブルに対してクエリを投げた時を例にする

```sql
CREATE TABLE users (
 id VARCHAR,
 first_name VARCHAR,
 last_name VARCHAR,
 gender VARCHAR,
 username VARCHAR,
 PRIMARY KEY (id),
 UNIQUE KEY username_UNIQUE (username)
)

CREATE TABLE orders (
 id VARCHAR,
 user_id VARCHAR,
 item VARCHAR,
 PRIMARY KEY (id),
 UNIQUE KEY user_id_UNIQUE (user_id)
)
```

### プライマリキーでの検索

- PRIMARY KEY なので、TableScan が B+Tree のキー検索で直接条件を処理でき、Filter を経由しない

```sql
-- id は PRIMARY KEY
SELECT first_name, last_name FROM users WHERE id = 1;
```

```txt
Project (first_name, last_name)
  └── TableScan (id = 1)
```

### インデックスなしカラムでの検索

- インデックスがないため、TableScan はフルスキャンしか行えず、フィルタリングは上位の Filter ノードが担当する

```sql
-- gender にインデックスなし
SELECT * FROM users WHERE gender = 'male';
```

```txt
Project (*)
  └── Filter (gender = 'male')
        └── TableScan (フルスキャン)
```

### ユニークインデックスでの検索

- UNIQUE INDEX があるため IndexScan を利用して検索できる
- SELECT * なので全カラムが必要となり、IndexScan はインデックスからマッチするエントリを見つけた後、そこに含まれる PK でテーブル本体を検索して全カラムを取得する

```sql
-- username に UNIQUE INDEX がある
SELECT * FROM users WHERE username = 'alice';
```

```txt
Project (*)
  └── IndexScan (username = 'alice')
```

### 複数インデックスの OR 検索

- それぞれにインデックスがあるため、各スキャン結果を Union で結合できる
- TableScan: PK を利用して `id = 1` を検索
- IndexScan: セカンダリインデックスを利用して `username = 'alice'` を検索
- Union: 両方の結果を結合し、重複を除去
- Project: 検索結果から全カラムを取り出す

```sql
-- id は PRIMARY KEY、username に UNIQUE INDEX がある
SELECT * FROM users WHERE id = 1 OR username = 'alice';
```

```txt
Project (*)
  └── Union
        ├── TableScan (id = 1)
        └── IndexScan (username = 'alice')
```

### JOIN + WHERE 条件

- users が駆動表に選ばれ、orders は UNIQUE INDEX で eq_ref アクセスする
<!-- TODO: リンク修正 -->
- WHERE 条件 (`orders.item = 'apple'`) は内部表のカラムなので駆動表側に分離できず、結合後に Filter で適用される (詳細: [プランナー - WHERE 条件の分離](../planner/planner.md#where-条件の分離))

```sql
-- orders.user_id に UNIQUE INDEX がある
-- users が orders より行数が少ない
SELECT * FROM users
INNER JOIN orders ON users.id = orders.user_id
WHERE orders.item = 'apple';
```

```txt
Project (*)
  └── Filter (orders.item = 'apple')
        └── NestedLoopJoin
              ├── TableScan (users: フルスキャン)
              └── IndexScan (orders: user_id UNIQUE INDEX で eq_ref)
```

### JOIN (WHERE なし)

- SELECT * なので全カラムが必要となり、内部表 (orders) の IndexScan はインデックスからマッチするエントリを見つけた後、PK でテーブル本体を検索して全カラムを取得する

```sql
-- orders.user_id に UNIQUE INDEX がある
-- users が orders より行数が少ない
SELECT * FROM users
INNER JOIN orders ON users.id = orders.user_id;
```

```txt
Project (*)
  └── NestedLoopJoin
        ├── TableScan (users: フルスキャン)
        └── IndexScan (orders: user_id UNIQUE INDEX で eq_ref)
```

### カバリングインデックス

- SELECT カラム (id, username) は PK + UNIQUE INDEX カラムだけで構成されるため、index-only scan が適用される
  - インデックスのリーフにはセカンダリキーと PK の値が格納されているので、テーブル本体を読まずにインデックスだけで結果を返せる
- IndexScan は内部モード (`indexOnly`) で index-only scan を行う
  - 独立したノード型ではなく、IndexScan のフラグで制御する

```sql
-- id は PRIMARY KEY、username に UNIQUE INDEX がある
SELECT id, username FROM users WHERE username = 'alice';
```

```txt
Project (id, username)
  └── IndexScan (username = 'alice', index-only)
```
