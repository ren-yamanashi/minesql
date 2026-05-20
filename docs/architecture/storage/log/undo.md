## Undo ログ

## 参考文献

- [InnoDB Undo Tablespaces](https://dev.mysql.com/doc/refman/8.0/ja/innodb-undo-tablespaces.html)
- [An In-Depth Analysis of Undo Logs in InnoDB](https://www.alibabacloud.com/blog/an-in-depth-analysis-of-undo-logs-in-innodb_598966) - UNDO ページヘッダー、セグメントヘッダー、レコードの詰め方、ページリストの構造
- [The basics of the InnoDB undo logging and history system](https://blog.jcole.us/2014/04/16/the-basics-of-the-innodb-undo-logging-and-history-system/) - UNDO ログの概念モデル、ヒストリリスト、パージの仕組み
- [INNODB_BUFFER_PAGE Table](https://dev.mysql.com/doc/refman/8.0/en/information-schema-innodb-buffer-page-table.html) - バッファプール内のページ一覧に UNDO_LOG が PAGE_TYPE として含まれている (UNDO ページがバッファプールで管理される根拠)
- [Improvements to Undo Truncation in MySQL 8.0.21](https://dev.mysql.com/blog-archive/improvements-to-undo-truncation-in-mysql-8-0-21/) - UNDO ページのバッファプールからのフラッシュと LRU による管理について言及

## 概要

- Undo ログは、トランザクションのロールバックを実現するための仕組み
  - ACID 農地 Atomicity を実現するための仕組み
- Undo ログには、トランザクションごとに複数の Undo ログレコードが記録される
- Undo ログレコードには、各操作に対してそれを取り消すために必要な情報が記録される
- ロールバックの際は、Undo ログに記録された Undo ログレコードを逆順に適用する

### Undo ログレコードに記録される内容

| 操作 | レコードに記録される内容 |
| --- | --- |
| Insert | 挿入したレコードの内容 |
| Delete | 削除前のレコードの内容 |
| Update (PK 不変) | 更新前のレコードの内容と更新後のレコードの内容 |
| Update (PK 変更) | Insert と Delete の 2 操作で記録されるレコードの内容 |

## データ構造

- Undo ログは、Undo ページに記録される
- Undo ページは `undo.db` ファイルに永続化される
- キャッシュにはデータページと同じバッファプールを使用

### Undo ログページ

- Undo レコードを格納するためのページ
- B+Tree ではなく単純な双方向リンクリスト (ページが満杯になると新しいページが割り当てられる)

| 領域 | バイト数 | 説明 |
| --- | --- | --- |
| ヘッダー | 2 | ボディ部分の使用済みバイト数 |
| ヘッダー | 4 | 次の Undo ページの PageNumber |
| ボディ | 可変 | Undo レコードが先頭に順から詰められる |

### Undo ログレコード

| offset | バイト数 | 項目 | 説明 |
| --- | --- | --- | --- |
| 0 | 4 | トランザクション ID | このレコードを記録したトランザクションの ID |
| 4 | 4 | Undo 番号 | トランザクション内の連番 |
| 8 | 1 | レコード種別 | 操作の種別 (1=Insert, 2=Delete, 3=Update) |
| 9 | 2 | データ長 | 変更内容のバイト数 |
| 11 | 可変 | 変更内容 | レコード種別に応じたデータ |

変更内容の先頭には、この操作で上書きされる前のレコードが持っていた `lastTrxId` と `rollPtr` が記録される\
これにより、Undo ログを辿って旧バージョンを参照できる (詳細: [トランザクション - 可視性の判定](../access/mvcc.md#可視性の判定))

#### 変更内容

| offset | サイズ | 項目 | 説明 |
| --- | --- | --- | --- |
| 0 | 4 バイト | prevLastModified | 上書き前の行の `lastModified` |
| 4 | 4 バイト | prevRollPtr | 上書き前の行の `rollPtr` |
| 8 | 4 バイト | FileId | テーブルの FileId |
| 12 | 可変 | カラムセット | カラムセット (下記参照) |

#### カラムセット

カラムセットは操作種別に応じて 1 つまたは 2 つ格納される

- Insert / Delete: 1 セット (操作対象の行)
- Update (PK 不変): 2 セット (更新前の行、更新後の行)

各カラムセットの形式

| サイズ | 項目 | 説明 |
| --- | --- | --- |
| 2 バイト | カラム数 | セット内のカラムの数 |
| 可変 | カラムデータ | 以下の形式をカラム数だけ繰り返す |

各カラムデータの形式

| サイズ | 項目 | 説明 |
| --- | --- | --- |
| 2 バイト | データ長 | カラムデータのバイト数 |
| 可変 | データ | カラムの値 |
