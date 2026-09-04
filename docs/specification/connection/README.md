# コネクションハンドラーの仕様

X Plugin のコネクションハンドラーの仕様を以下の文書に分けて記述\
「X Plugin の要件 → 論理モデル → 詳細」の順で読むことを想定

1. [x_plugin.md](./x_plugin.md): コネクションハンドラーを含む X Plugin 全体の要件
2. [connection_handler.md](./connection_handler.md): コネクションハンドラーの論理モデル
   - 責務、処理の流れ、構成要素、スレッドモデル、接続とセッションの状態遷移、タイムアウトと上限の種類
3. [connection_handler_spec.md](./connection_handler_spec.md): コネクションハンドラーの詳細仕様
   - 起動と接続の待ち受け、接続の受付、ワーカースレッドプール、接続のライフサイクル、タイムアウトと強制切断、設定変数とステータス変数
