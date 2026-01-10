# エグゼキュータ

- 実装コード:
  - [executor/sequential_scan.go](../../internal/executor/sequential_scan.go)
  - [executor/record.go](../../internal/executor/record.go)

## 概要

- エグゼキュータは、プランナーに指定された通りにクエリを実行する
  - それぞれのエグゼキュータは、「範囲検索をするもの」「カラム値を利用して検索するもの」など役割ごとに分かれている
  - それらを組み合わせたものが「プランナ」になる
