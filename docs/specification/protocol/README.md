# X Protocol の仕様

X Protocol の仕様を以下の 3 つの文書に分けて記述\
「概要 → 流れ → 詳細」の順で読むことを想定

1. [x_protocol.md](./x_protocol.md): X Protocol の概要
   - プロトコルの位置づけ、Protocol Buffers、メッセージ構造 (フレーム)
2. [communication_flow.md](./communication_flow.md): 通信の流れ
   - 接続確立から終了までの全体像、メッセージのやり取りの規則、SQL 実行の流れ
3. [message_spec.md](./message_spec.md): 各メッセージの詳細仕様
   - 型定義ファイルの構成、汎用データ型、各フェーズの詳細、リザルトセットのエンコーディング、エラー、Notice、メッセージ種別の一覧
   - 他のドメインの詳細仕様と違い、外部との契約 (実クライアントがそのまま前提にするメッセージ定義とエンコーディング) なので読む順に含める
