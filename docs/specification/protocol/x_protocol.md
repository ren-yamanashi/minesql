# X プロトコル

## 概要

- MySQL の X プロトコルは MySQL 5.7.12 で導入された MySQL の次世代通信プロトコル
- デフォルトではポート 33060 で待機
- MySQL 8.4 (LTS) の X プロトコルは、Protocol Buffers を使用してデータをシリアライズする
  - これにより、コンパクトなバイナリエンコーディングとプロトコルメッセージの明確に定義されたスキーマが提供され、言語間でのコネクタの実装が容易になる
  - 型の定義は https://github.com/mysql/mysql-server/tree/8.4/plugin/x/protocol/protobuf

### Protocol Buffers (protobuf)

- Protocol Buffers (protobuf) は、Google が開発した軽量で効率的なデータシリアライズフォーマット
- JSONやXML のようなフォーマットと同様に、データの構造を定義し、異なるシステム間でのデータ交換を効率化することを目的としている
- バイナリ形式なので、JSON や XML よりもデータサイズが小さく高速
- `.proto` ファイルに型安全にデータ構造を定義できる

## 主な特徴

- 最初のレスポンスが届く前に複数のリクエストをサーバーへ送信できるため、高スループットが必要な状況においてラウンドトリップのレイテンシを削減できる
- このプロトコルは、再接続することなく単一の接続上でセッションのリセットや再利用を行う機能を備えており、接続設定にかかるオーバーヘッドを軽減する
- プロトコルに新しいメッセージタイプを追加しても古いクライアントの動作が妨げられないため、前方互換性が確保される

## メッセージ構造 (フレーム)

- すべてのメッセージは以下のフレーム構造で送受信される
  - 参照: [mysqlx.proto 冒頭の doc コメント `@section messages_Message_Structure`](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/plugin/x/protocol/protobuf/mysqlx.proto#L48-L70)

- 補足
  - フレーム=送受信の単位で、下記の `length` + `message_type` + `message_payload` の 1 まとまり
  - TCP はメッセージの境界を持たない単なるバイトストリームであるため、先頭の `length` を使って受信側が「どこまでが 1 メッセージか」を切り出す
  - また、フレームはあくまで論理的な区切りであり、TCP レイヤーのパケット分割とは対応しない (1 つのフレームが複数のパケットに分かれて届くことも、複数のフレームが 1 つのパケットにまとまって届くこともある)
  - そのため受信側は `length` 分のバイトが揃うまで読み溜めてから 1 フレームとして切り出す

```txt
struct Message {
  uint32 length;                              // 4 バイト、リトルエンディアン
  uint8  message_type;                        // 1 バイト
  opaque message_payload[Message.length - 1]; // protobuf でエンコードされたペイロード
};
```

- length
  - `message_type` (1 バイト) と `message_payload` の長さの合計 (`length` フィールド自身の 4 バイトは含まない)
- message_type
  - クライアント発なら `ClientMessages.Type`、サーバー発なら `ServerMessages.Type` の enum 値
- message_payload
  - `message_type` に対応する protobuf メッセージをシリアライズしたバイト列

### 補足

- 1 つのフレームに入る protobuf メッセージは 1 個だけ
- protobuf のシリアライズ結果は型情報を含まないため、受信側は `message_type` の値を見てペイロードをどの型としてデシリアライズするかを決める
- `ClientMessages` / `ServerMessages` は「種別値とメッセージ型の対応表」を enum として定義したもの (protoc に定数生成と ID の一意性チェックをさせるための定義) で、通信路上を流れるデータではない
  - クライアントとサーバーは同じ `.proto` ファイルからコードを生成してビルドされるため、この対応表は最初から双方のプログラムに埋め込まれている (通信で共有する必要がなく、接続時に 1 度送られる、といったこともない)
  - 実行時に通信路上を流れるのは、各フレームの先頭に書き込まれる 1 バイトの種別値のみ
- この対応表は `.proto` ファイル内では 2 段階で宣言されている
  - [mysqlx.proto](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/plugin/x/protocol/protobuf/mysqlx.proto) の enum が種別値と種別名を対応付けている (例: `CON_CAPABILITIES_GET = 1;`)
  - 各メッセージ定義の末尾にある `option (client_message_id)` / `option (server_message_id)` が、そのメッセージ型と種別名を対応付けている (例: `message CapabilitiesGet { option (client_message_id) = CON_CAPABILITIES_GET; }`)
  - この 2 つをつなぐことで「`CapabilitiesGet` ⇔ 種別値 1」という対応が得られる
