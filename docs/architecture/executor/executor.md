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
  - IndexScan: インデックスを利用して検索する
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
SELECT first_name, last_name FROM users WHERE id = 1;
```

この場合、PRIMARY KEY のインデックスを使って検索できるため、Filter を経由せずに TableScan 自体が条件を処理できる。そのためエグゼキュータのツリーは以下のようになる

- TableScan: テーブルに対して検索をかける
  - 条件: `id = 1`
- Project: 検索結果から特定のカラムだけを取り出す
  - 対象カラム: `first_name`, `last_name`

```txt
Project (first_name, last_name)
  └── TableScan (id = 1)
```

### 例2

```sql
SELECT * FROM users WHERE gender = 'male';
```

この場合、gender にインデックスがないため、TableScan はフルスキャンしか行えず、フィルタリングは上位の Filter ノードが担当する。  
そのためエグゼキュータのツリーは以下のようになる

- Filter: テーブルに対して検索をかける
  - 条件: `gender = 'male'`
- TableScan: テーブルに対して検索をかける
  - 条件: フルテーブルスキャン
- Project: 検索結果から特定のカラムだけを取り出す
  - 対象カラム: `*` (全てのカラム)

```txt
Project (*)
  └── Filter (gender = 'male')
        └── TableScan (フルスキャン)
```

### 例3

```sql
SELECT * FROM users WHERE username = 'alice';
```

この場合、username にユニークインデックスがあるため、TableScan はフルスキャンを行う必要はなく、IndexScan を利用して検索できる。  
そのためエグゼキュータのツリーは以下のようになる

- IndexScan: インデックスを利用して検索する
  - 条件: `username = 'alice'`
- Project: 検索結果から特定のカラムだけを取り出す
  - 対象カラム: `*` (全てのカラム)

```txt
Project (*)
  └── IndexScan (username = 'alice')
```

### 例4

```sql
SELECT * FROM users WHERE id = 1 OR username = 'alice';
```

この場合、id は PRIMARY KEY、username にはユニークインデックスがあるため、それぞれのスキャン結果を Union で結合できる。

- TableScan: PK を利用して `id = 1` を検索
- IndexScan: ユニークインデックスを利用して `username = 'alice'` を検索
- Union: 両方の結果を結合し、重複を除去
- Project: 検索結果から全カラムを取り出す

```txt
Project (*)
  └── Union
        ├── TableScan (id = 1)
        └── IndexScan (username = 'alice')
```

### ツリー図

全ての Executor は共通の `Executor` interface (`Next() (Record, error)`) を実装する。
一部の Executor は `InnerExecutor` を持ち、子ノードから `Next()` でデータを受け取って処理する。

```txt
Executor (interface)
  │
  ├── TableScan          (リーフノード: テーブル全体を走査する)
  ├── IndexScan          (リーフノード: セカンダリインデックスを利用して検索する)
  │
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
