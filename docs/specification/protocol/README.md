# X Protocol

## 仕様

1. X Protocol の概要: [x_protocol.md](./x_protocol.md)
   - プロトコルの位置づけ、Protocol Buffers、メッセージ構造 (フレーム)
2. 通信の流れ: [communication_flow.md](./communication_flow.md)
   - 接続確立から終了までの全体像、メッセージのやり取りの規則、SQL 実行の流れ
3. 各メッセージの詳細仕様: [message_spec.md](./message_spec.md)
   - 型定義ファイルの構成、汎用データ型、capability・認証・セッション終了・SQL 実行の各メッセージ、リザルトセットのエンコーディング、エラー、Notice、Expect ブロック、実装しない機能、メッセージ種別の一覧
   - 外部との契約 (実クライアントがそのまま前提にするメッセージ定義とエンコーディング) なので、概要と流れに続けて読む
