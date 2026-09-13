# コマンドディスパッチャ

## 仕様

1. コマンドディスパッチャを含む X Plugin 全体の要件: [connection/x_plugin.md](../connection/x_plugin.md)
2. コマンドディスパッチャの論理モデル: [command_dispatcher.md](./command_dispatcher.md)
   - 責務、構成要素、実行モデル、処理の流れ、保証する規則、Expect ブロック、管理コマンド

## その他 (実装時の参照資料)

- [reference/command_dispatcher_spec.md](./reference/command_dispatcher_spec.md)
  - コマンドディスパッチャの詳細仕様 (種別ごとの振り分け、`StmtExecute` の処理、管理コマンド、Expect ブロック、SQL 層での実行、エラーと深刻度、状態変数) と、論理モデルの主張に対応する MySQL のソース
  - 論理モデルと食い違う場合は論理モデルを正とし、そのドメインを実装する段階で更新する
