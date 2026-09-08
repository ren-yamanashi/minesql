# コマンドディスパッチャの仕様

X Plugin のコマンドディスパッチャの仕様を以下の文書に分けて記述\
「論理モデル → 詳細」の順で読むことを想定

X Plugin 全体の要件は [connection/x_plugin.md](../connection/x_plugin.md) を参照

1. [command_dispatcher.md](./command_dispatcher.md): コマンドディスパッチャの論理モデル
   - 責務、構成要素、実行モデル、処理の流れ、保証する規則、Expect ブロック、管理コマンド
2. [command_dispatcher_spec.md](./command_dispatcher_spec.md): コマンドディスパッチャの詳細仕様
   - 種別ごとの振り分け、`StmtExecute` の処理、管理コマンド、Expect ブロック、SQL 層での実行、エラーと深刻度、状態変数
