# コネクションハンドラー

## 仕様

1. コネクションハンドラーを含む X Plugin 全体の要件: [x_plugin.md](./x_plugin.md)
2. コネクションハンドラーの論理モデル: [connection_handler.md](./connection_handler.md)
   - 責務、処理の流れ、構成要素、スレッドモデル、接続とセッションの状態遷移、タイムアウトと上限の種類
3. 認証の論理モデル: [authentication.md](./authentication.md)
   - 責務、構成要素 (認証ハンドラ、アカウント照合、SHA256 パスワードキャッシュ、内部セッションの実行ユーザー)、処理の流れ、層ごとの分担、MySQL の要件と MineSQL での対応範囲
