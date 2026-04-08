# UNDO ログ

## 参考文献

- [InnoDB Undo Tablespaces](https://dev.mysql.com/doc/refman/8.0/ja/innodb-undo-tablespaces.html)
- [An In-Depth Analysis of Undo Logs in InnoDB](https://www.alibabacloud.com/blog/an-in-depth-analysis-of-undo-logs-in-innodb_598966) - UNDO ページヘッダー、セグメントヘッダー、レコードの詰め方、ページリストの構造
- [The basics of the InnoDB undo logging and history system](https://blog.jcole.us/2014/04/16/the-basics-of-the-innodb-undo-logging-and-history-system/) - UNDO ログの概念モデル、ヒストリリスト、パージの仕組み
- [INNODB_BUFFER_PAGE Table](https://dev.mysql.com/doc/refman/8.0/en/information-schema-innodb-buffer-page-table.html) - バッファプール内のページ一覧に UNDO_LOG が PAGE_TYPE として含まれている (UNDO ページがバッファプールで管理される根拠)
- [Improvements to Undo Truncation in MySQL 8.0.21](https://dev.mysql.com/blog-archive/improvements-to-undo-truncation-in-mysql-8-0-21/) - UNDO ページのバッファプールからのフラッシュと LRU による管理について言及

## 概要

- UNDO ログは、トランザクションのロールバックを実現するための仕組み
  - ACID のうち、Atomicity を実現するための仕組み
- UNDO ログには、トランザクションごとに複数の UNDO ログレコードが記録される
  - UNDO ログレコードには、各操作に対してそれを取り消すために必要な情報を記録する (以下)

| 操作 | レコードに記録される内容 |
| --- | --- |
| INSERT | 挿入した行の内容 |
| DELETE | 削除前の行の内容 |
| UPDATE (PK 不変) | 更新前の行の内容と更新後の行の内容 |
| UPDATE (PK 変更) | INSERT と DELETE の 2 レコード |

- ロールバックの際には、UNDO ログに記録されたレコードを逆順に適用する

## データ構造

UNDO ログは UNDO ログ専用のファイル (`undo.log`) にレコードを記録する。

#### ファイル構成

```txt
|  ファイルヘッダー  | レコード | レコード | レコード | ... |
0                 16
```

#### ファイルヘッダー

- サイズ: 16 バイト

| offset | サイズ | 項目 | 説明 |
| --- | --- | --- | --- |
| 0 | 8 バイト | 末尾オフセット | 最後のレコードの末尾位置 |
| 8 | 8 バイト | 予約領域 | 将来の拡張用 |

#### UNDO ログレコード

| offset | サイズ | 項目 | 説明 |
| --- | --- | --- | --- |
| 0 | 8 バイト | トランザクション ID | このレコードを記録したトランザクションの ID |
| 8 | 8 バイト | UNDO 番号 | トランザクション内の連番 (ロールバック時の逆順適用に使用) |
| 16 | 1 バイト | レコード種別 | 操作の種別 (INSERT, DELETE, UPDATE_INPLACE) |
| 17 | 2 バイト | データ長 | 変更内容のバイト数 |
| 19 | 可変 | 変更内容 | レコード種別に応じたデータ (対象テーブル名、行の内容など) |

## 永続化のタイミング

UNDO ログは通常のデータページと同様にバッファプール上で管理され、以下の流れでディスクに永続化される。

1. データ変更時: UNDO ページへの書き込みを REDO ログバッファに記録する (メモリ)
2. データ変更時: UNDO レコードをバッファプール上の UNDO ページに書き込む (メモリ)
3. (必要に応じて) バッファプールの都合で UNDO ページがディスクにフラッシュされる (その前に WAL 原則により関連する REDO ログが先にディスクに書かれる)

UNDO ページ自体のディスクフラッシュはバッファプールの都合で行われるため、COMMIT 時点では UNDO ページはまだメモリにしかない可能性がある。\
しかし UNDO ページへの変更は REDO ログに記録されており、REDO ログは COMMIT 時にディスクにフラッシュされる。したがってクラッシュリカバリ時に REDO ログを適用すれば UNDO ページを復元できる。

また、COMMIT 前であっても、データページがバッファプールの都合でディスクにフラッシュされた場合、LSN の順序保証により UNDO ページの REDO ログも必ず先にディスクに書かれている。\
WAL 原則によりデータページのフラッシュ前にそのページの REDO ログがフラッシュされるが、REDO ログはシーケンシャルに書かれるため、データページの REDO ログより前に記録された UNDO ページの REDO ログも一緒にフラッシュされる (詳細: [REDO ログ - UNDO ページの REDO ログが必ずディスクに書かれる理由](./redo.md#undo-ページの-redo-ログが必ずディスクに書かれる理由))。

つまり UNDO ログの永続性は REDO ログによって保護される。

詳細: [REDO ログ - クラッシュリカバリの流れ](./redo.md#クラッシュリカバリの流れ)
