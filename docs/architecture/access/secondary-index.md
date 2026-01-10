# セカンダリインデックス

- 該当コード:
  - [storage/access/table/unique_index.go](../../../internal/storage/access/table/unique_index.go)

## 概要

- セカンダリインデックスには、セカンダリインデックスを構成するカラム値を key, プライマリキーを value として格納する
- 検索の際は、まずセカンダリインデックスを検索し、(必要であれば) その value であるプライマリキーを使って、実際のレコードをテーブルから取得する
