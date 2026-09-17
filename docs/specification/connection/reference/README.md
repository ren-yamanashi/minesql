# コネクションハンドラーの参照資料 (実装時)

- [connection_handler_spec.md](./connection_handler_spec.md)
  - コネクションハンドラーの詳細仕様 (起動と接続の待ち受け、接続の受付、ワーカースレッドプール、接続のライフサイクル、タイムアウトと強制切断、設定変数とステータス変数) と、論理モデルの主張に対応する MySQL のソース
  - 論理モデルと食い違う場合は論理モデルを正とし、そのドメインを実装する段階で更新する
- [authentication_spec.md](./authentication_spec.md)
  - 認証の論理モデルの主張に対応する MySQL のソース
