# カタログ

- 実装コード
  - [internal/storage/access/catalog/catalog.go](../../../internal/storage/access/catalog/catalog.go)
  - [internal/storage/access/catalog/column_metadata.go](../../../internal/storage/access/catalog/column_metadata.go)
  - [internal/storage/access/catalog/index_metadata.go](../../../internal/storage/access/catalog/index_metadata.go)
  - [internal/storage/access/catalog/table_metadata.go](../../../internal/storage/access/catalog/table_metadata.go)

## 概要

- テーブルのメタデータ (テーブル名、カラム情報、インデックス情報など) を管理する
- カタログは実データとは別のファイル (`minesql.db`) に保存される
- カタログは以下の 3 つの B+Tree で構成される
  - テーブルメタデータ B+Tree
  - インデックスメタデータ B+Tree
  - カラムメタデータ B+Tree

### ヘッダー

- カタログファイルの先頭ページはヘッダーページとしている
- ヘッダーページの構成は以下
  - オフセット 0-3: マジックナンバー
  - オフセット 4-7: テーブルメタデータの B+Tree のページ番号
  - オフセット 8-11: インデックスメタデータの B+Tree のページ番号
  - オフセット 12-15: カラムメタデータの B+Tree のページ番号
  - オフセット 16-23: 次に割り当てるテーブル ID

### カラム

- 現状は文字列型 (string) のみをサポート
