# エグゼキュータ

## 概要

- エグゼキュータは、プランナーに指定された通りにクエリを実行する
  - それぞれのエグゼキュータは、「範囲検索をするもの」「カラム値を利用して検索するもの」「レコードを追加するもの」など役割ごとに分かれている
  - それらの種類のエグゼキュータを組み合わせて、一つの大きなエグゼキュータを構成する
  - 組み合わさったエグゼキュータは、ツリー構造をしている

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
  - Project: 検索結果から特定のカラムだけを取り出す

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
