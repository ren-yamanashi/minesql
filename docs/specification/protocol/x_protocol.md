# X プロトコル

## 概要

- MySQL の X プロトコルは MySQL 5.7.12 で導入された MySQL の次世代通信プロトコル
  - 参照:
    - [mysqlx.proto の doc コメント (MySQL Server 5.7.12 or higher)](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/plugin/x/protocol/protobuf/mysqlx.proto#L321-L324)
- デフォルトではポート 33060 で待機
- MySQL 8.4 (LTS) の X プロトコルは、Protocol Buffers を使用してデータをシリアライズする
  - これにより、コンパクトなバイナリエンコーディングとプロトコルメッセージの明確に定義されたスキーマが提供され、言語間でのコネクタの実装が容易になる
  - 型の定義は https://github.com/mysql/mysql-server/tree/aa461240270d809bcac336483b886b3d1789d4d9/plugin/x/protocol/protobuf

### Protocol Buffers (protobuf)

- Protocol Buffers (protobuf) は、Google が開発した軽量で効率的なデータシリアライズフォーマット
- JSON や XML のようなフォーマットと同様に、データの構造を定義し、異なるシステム間でのデータ交換を効率化することを目的としている
- バイナリ形式なので、JSON や XML よりもデータサイズが小さく高速
- `.proto` ファイルに型安全にデータ構造を定義できる

## X Protocol / X Plugin / X DevAPI の関係

X Protocol の周辺には名前の似た用語が 3 つあり、それぞれ層が違う

- X Protocol: プロトコル (通信規約)
  - TCP (既定ポート 33060) または Unix ソケット上を流れるフレーム + protobuf メッセージの規約
- X Plugin: サーバー側の実装
  - X Protocol を待ち受けるサーバー側エンドポイントで、接続受付・capability・認証・Notice などのプロトコル処理全般を担う
  - 素の SQL 実行 (`StmtExecute`) に加えて、MySQL をドキュメントストアとして使うためのドキュメントモデルインターフェースを提供する
    - CRUD メッセージ (`Mysqlx.Crud.Find` / `Insert` / `Update` / `Delete`) や `create_collection` などの管理コマンドを、JSON カラムを持つ通常の InnoDB テーブルに対する SQL に変換して実行する
    - JSON 型や JSON 関数は X Plugin ではなく MySQL サーバーが元々持つ機能であり、X Plugin が足しているのは「SQL を書かずにそれを操作できるプロトコル面」のみ
- X DevAPI: クライアント側の API 仕様
  - MySQL Shell や各言語の Connector が実装する API で、内部で X Protocol を話す
  - ドキュメント (コレクション) とリレーショナルテーブルの双方を、SQL を書かずにメソッドチェーン形式の CRUD で操作できる
    - コレクションへの操作は上記の CRUD メッセージとしてサーバーに送られる (= ドキュメントモデルインターフェースのクライアント側の入り口)
  - X DevAPI を使わずに、`.proto` からコードを生成して X Protocol を直接話すこともできる

## 主な特徴

- 最初のレスポンスが届く前に複数のリクエストをサーバーへ送信できるため、高スループットが必要な状況においてラウンドトリップのレイテンシを削減できる
  - 参照:
    - [mysqlx-protocol-implementation.dox の Pipelining](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/plugin/x/protocol/doc/mysqlx-protocol-implementation.dox#L68-L71)
- このプロトコルは、再接続することなく単一の接続上でセッションのリセットや再利用を行う機能を備えており、接続設定にかかるオーバーヘッドを軽減する
  - 参照:
    - [mysqlx-protocol-lifecycle.dox のセッション操作](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/plugin/x/protocol/doc/mysqlx-protocol-lifecycle.dox#L102-L112)
- capability の合意や無視可能な Notice といった仕組みにより、古いクライアントを壊さずにプロトコルを拡張できる
  - 参照:
    - [mysqlx-protocol-notices.dox (Notice は無視してもシーケンスの理解に影響しない)](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/plugin/x/protocol/doc/mysqlx-protocol-notices.dox#L40-L42)
    - [mysqlx-protocol-comparison.dox の比較表](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/plugin/x/protocol/doc/mysqlx-protocol-comparison.dox#L29-L45)

## classic protocol (Client/Server プロトコル) との違い

主な違いは、classic protocol が持つ以下の制約を X プロトコルがどう解消したか、として整理できる

- メッセージの解釈
  - classic はメッセージの解釈が文脈に依存する
    - ペイロードは独自のバイナリ形式で、応答の種類もペイロード先頭のマーカーバイトを「どのコマンドへの応答か」という文脈に応じて読み分ける必要があり、この読み分けを各言語のクライアントが手で実装することになる
  - X プロトコルはメッセージの構造定義を `.proto` ファイルに集約し、フレームのヘッダに種別バイトを持たせた
    - これにより、どのメッセージも「種別を見て、対応する型としてデシリアライズする」という文脈に依存しない一様な処理で受信でき、エンコード・デコードの実装もコード生成で自動化できる
  - 参照:
    - [sql-common/net_serv.cc のパケット構造の doc コメント](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/sql-common/net_serv.cc#L338-L427)
    - [sql/protocol_classic.cc のコマンドフェーズの doc コメント](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/sql/protocol_classic.cc#L155-L181)
- 要求と応答の対応付け
  - classic は 1 コマンド送って応答を受け取ってから次を送る前提で、コマンドのたびに往復の待ちが発生する
  - X プロトコルはやり取りを「シーケンス」(= 1 つの要求 + それに続く応答の列) という単位で規定しており、応答を待たずに次の要求を送っても、応答の列が要求の順に返ることで対応関係が崩れない
    - そのためパイプライン化ができ、往復のレイテンシを削減できる
  - 参照:
    - [mysqlx.proto の `@section messages_Message_Sequence`](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/plugin/x/protocol/protobuf/mysqlx.proto#L103-L118)
    - [mysqlx-protocol-implementation.dox の Pipelining](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/plugin/x/protocol/doc/mysqlx-protocol-implementation.dox#L68-L71)
- capability の表現
  - classic の capability はハンドシェイクパケット内のビットフラグで、2 つの制約がある
    - ビットは真偽値しか表せないため、「どのアルゴリズムか」「レベルはいくつか」のような値を伝えるには、フラグと連動してハンドシェイクパケットに追加フィールドを差し込む場当たり的な拡張が必要になる (実例: zstd 圧縮のレベルは、フラグが立っているときだけパケットに追加される 1 バイトで運ばれる)
    - 32 ビットしかなく、実際にほぼ枯渇している (bit 29 は「64 ビットへの拡張のための予約」になっている)
  - X プロトコルは capability を「名前 + 型付きの値」の組として専用メッセージ (`CapabilitiesGet` / `CapabilitiesSet`) で交換するため、値や構造化データもそのまま運べ、新しい capability を名前の追加だけで導入できる
  - 参照:
    - [include/mysql_com.h の zstd 圧縮フラグ](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/include/mysql_com.h#L699-L716)
    - [sql/auth/sql_authentication.cc の zstd レベルの追加フィールド](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/sql/auth/sql_authentication.cc#L3226-L3230)
    - [include/mysql_com.h の 64 ビット拡張予約](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/include/mysql_com.h#L751-L755)
- サーバー起点の通知
  - classic でサーバーが自分から送るのは接続直後のハンドシェイクだけで、以降は要求への応答しか返せない
  - X プロトコルは応答とは独立な通知 (Notice) を持つため、警告や状態変化をやり取りの途中でも運べる
  - 参照:
    - [mysqlx-protocol-comparison.dox の比較表 (out-of-band notifications)](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/plugin/x/protocol/doc/mysqlx-protocol-comparison.dox#L38)
- 接続直後の手順
  - classic は接続直後の手順が「サーバー主導のハンドシェイクの中で capability 交換から認証まで一続きに行う」形に固定されている
  - X プロトコルは接続直後の義務的な手続きがなく (サーバーは `ServerHello` の Notice を送るだけ)、capability ネゴシエーション (任意) も認証もクライアントが自分のタイミングで開始する
  - 参照:
    - [sql/auth/sql_authentication.cc の接続フェーズの doc コメント](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/sql/auth/sql_authentication.cc#L127-L190)
    - [mysqlx-protocol-lifecycle.dox の Stages of Session Setup](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/plugin/x/protocol/doc/mysqlx-protocol-lifecycle.dox#L118-L128)

## メッセージ構造 (フレーム)

- すべてのメッセージは以下のフレーム構造で送受信される
  - 参照:
    - [mysqlx.proto 冒頭の doc コメント `@section messages_Message_Structure`](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/plugin/x/protocol/protobuf/mysqlx.proto#L48-L70)

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

## 参考文献

- https://dev.mysql.com/doc/dev/mysql-server/latest/page_mysqlx_protocol.html
