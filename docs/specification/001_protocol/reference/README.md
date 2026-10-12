# X Protocol の参照資料 (実装時)

- [message_spec.md](./message_spec.md): 各メッセージの詳細仕様。`.proto` ファイルが定める契約 (メッセージの定義、値の型、エンコーディング、種別の一覧) で、MineSQL はこれにそのまま従う
- [x_plugin_behavior.md](./x_plugin_behavior.md): X Plugin のメッセージ処理の振る舞いの記録と、論理モデルの主張に対応する MySQL のソース
  - MineSQL も互換の要件として同じ応答をする
