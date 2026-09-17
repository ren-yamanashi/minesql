# コマンドディスパッチャの参照資料 (実装時)

- [command_dispatcher_spec.md](./command_dispatcher_spec.md)
  - コマンドディスパッチャの詳細仕様 (種別ごとの振り分け、`StmtExecute` の処理、管理コマンド、Expect ブロック、SQL 層での実行、エラーと深刻度、状態変数) と、論理モデルの主張に対応する MySQL のソース
  - 論理モデルと食い違う場合は論理モデルを正とし、そのドメインを実装する段階で更新する
