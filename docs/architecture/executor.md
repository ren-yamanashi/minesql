# エグゼキュータ

## 概要

- エグゼキュータは、プランナーに指定された通りにクエリを実行する
  - それぞれのエグゼキュータは、「範囲検索をするもの」「カラム値を利用して検索するもの」「レコードを追加するもの」など役割ごとに分かれている
- エグゼキュータは以下の二種類に分類される
  - RecordIterator
  - Mutator

## RecordIterator

- RecordIterator はレコードを逐次返す (iterable)
  - 条件に一致するレコードがなくなるまで、1行ずつレコードを返す

- RecordIterator は以下の 3 種類に分類される
  - SearchTable
  - SearchIndex
  - Filter

- SearchTable, SearchIndex では継続条件を満たすかどうかを判定し、満たさなくなるまでレコードを返す (継続条件を常に true にするとフルスキャンになる)
- Filter では、SearchTable や SearchIndex で取得したレコードの中から、条件に一致するレコードを絞り込む
- SearchTable, SearchIndex は "ここまでのデータを取得する" ことを担い、Filter は "取得したデータの中から必要なものを絞り込む" ことを担うイメージ

## Mutator

- Mutator はレコードの追加/削除/更新やテーブル作成などの副作用を実行する
- レコードは返さない (non-iterable)
