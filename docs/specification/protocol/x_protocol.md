# X Protocol

## 概要

- MySQL の X Protocol は MySQL 5.7.12 で導入された通信プロトコルで、従来の classic protocol と共存する
- X Protocol は、Protocol Buffers を使用してデータをシリアライズする
  - メッセージの構造を `.proto` ファイルで定義し、コンパクトなバイナリ形式で運ぶため、各言語のコネクタを同じ定義から生成できる
  - 型の定義は https://github.com/mysql/mysql-server/tree/aa461240270d809bcac336483b886b3d1789d4d9/plugin/x/protocol/protobuf

### Protocol Buffers (protobuf)

- Protocol Buffers (protobuf) は、Google が開発した軽量で効率的なデータシリアライズフォーマット
- JSON や XML のようなフォーマットと同様に、データの構造を定義し、異なるシステム間でのデータ交換を効率化することを目的としている
- バイナリ形式なので、JSON や XML よりもデータサイズが小さく高速
- `.proto` ファイルに型安全にデータ構造を定義できる

## X Protocol / X Plugin / X DevAPI の関係

X Protocol の周辺には名前の似た用語が 3 つあり、それぞれ層が違う

- X Protocol: 通信規約
  - TCP (既定ポート 33060) または Unix ソケット上を流れるフレーム + protobuf メッセージの規約
- X Plugin: サーバー側の実装
  - X Protocol を待ち受けるサーバー側エンドポイントで、接続受付・capability・認証・Notice などのプロトコル処理全般を担う
  - 素の SQL 実行 (`StmtExecute`) に加えて、MySQL をドキュメントストアとして使うためのドキュメントモデルインターフェースを提供する
    - CRUD メッセージ (`Mysqlx.Crud.Find` / `Insert` / `Update` / `Delete`) や `create_collection` などの管理コマンドを、JSON 列を持つ通常の InnoDB テーブルに対する SQL に変換して実行する
    - JSON 型や JSON 関数は X Plugin ではなく MySQL サーバーが元々持つ機能であり、X Plugin が足しているのは「SQL を書かずにそれを操作できるプロトコル面」のみ
- X DevAPI: クライアント側の API 仕様
  - MySQL Shell や各言語の Connector が実装する API で、内部で X Protocol を話す
  - ドキュメント (コレクション) とリレーショナルテーブルの双方を、SQL を書かずにメソッドチェーン形式の CRUD で操作できる
    - コレクションへの操作は上記の CRUD メッセージとしてサーバーに送られる (= ドキュメントモデルインターフェースのクライアント側の入り口)
  - X DevAPI を使わずに、`.proto` からコードを生成して X Protocol を直接話すこともできる

## 主な特徴

- 最初のレスポンスが届く前に複数のリクエストをサーバーへ送信できるため、高スループットが必要な状況において往復のレイテンシを削減できる
- 再接続することなく単一の接続上でセッションのリセットや再利用を行う機能を備えているので、接続設定にかかるオーバーヘッドを軽減する
- 使う機能はクライアントが名前で問い合わせて確かめ (capability)、サーバーからの追加情報は知らなければ読み飛ばせる形 (Notice) で届くので、古いクライアントを壊さずにサーバー側を拡張できる

## classic protocol (Client/Server プロトコル) との違い

主な違いは、classic protocol が持つ以下の制約を X Protocol がどう解消したか、として整理できる

- メッセージの解釈
  - classic はメッセージの解釈が文脈に依存する
    - ペイロードは独自のバイナリ形式で、レスポンスの種類もペイロード先頭のマーカーバイトを「どのコマンドへのレスポンスか」という文脈に応じて読み分ける必要があり、この読み分けを各言語のクライアントが手で実装することになる
  - X Protocol はメッセージの構造定義を `.proto` ファイルに集約し、フレームのヘッダに種別バイトを持たせた
    - これにより、どのメッセージも「種別を見て、対応する型としてデシリアライズする」という文脈に依存しない一様な処理で受信でき、エンコード・デコードの実装もコード生成で自動化できる
  - 参照:
    - [sql-common/net_serv.cc のパケット構造の doc コメント](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/sql-common/net_serv.cc#L338-L427)
    - [sql/protocol_classic.cc のコマンドフェーズの doc コメント](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/sql/protocol_classic.cc#L155-L181)
- リクエストとレスポンスの対応付け
  - classic は 1 コマンド送ってレスポンスを受け取ってから次を送る前提で、コマンドのたびに往復の待ちが発生する
  - X Protocol はやり取りを「シーケンス」(= 1 つのリクエスト + それに続くレスポンスの列) という単位で規定しており、レスポンスを待たずに次のリクエストを送っても、レスポンスの列がリクエストの順に返ることで対応関係が崩れない
    - そのためパイプライン化ができ、往復のレイテンシを削減できる
  - 参照:
    - [mysqlx.proto の `@section messages_Message_Sequence`](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/plugin/x/protocol/protobuf/mysqlx.proto#L103-L118)
    - [mysqlx-protocol-implementation.dox の Pipelining](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/plugin/x/protocol/doc/mysqlx-protocol-implementation.dox#L68-L71)
- capability の表現
  - classic の capability はハンドシェイクパケット内のビットフラグで、2 つの制約がある
    - ビットは真偽値しか表せないため、「どのアルゴリズムか」「レベルはいくつか」のような値を伝えるには、フラグと連動してハンドシェイクパケットに追加フィールドを差し込むフラグごとの個別の拡張が必要になる (実例: zstd 圧縮のレベルは、フラグが立っているときだけパケットに追加される 1 バイトで運ばれる)
    - 32 ビットしかなく、実際にほぼ枯渇している (bit 29 は「64 ビットへの拡張のための予約」になっている)
  - X Protocol は capability を「名前 + 型付きの値」の組として専用メッセージ (`CapabilitiesGet` / `CapabilitiesSet`) で交換するため、値や構造化データもそのまま運べ、新しい capability を名前の追加だけで導入できる
  - 参照:
    - [include/mysql_com.h の zstd 圧縮フラグ](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/include/mysql_com.h#L699-L716)
    - [sql/auth/sql_authentication.cc の zstd レベルの追加フィールド](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/sql/auth/sql_authentication.cc#L3226-L3230)
    - [include/mysql_com.h の 64 ビット拡張予約](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/include/mysql_com.h#L751-L755)
- サーバー起点の Notice
  - classic でサーバーが自分から送るのは接続直後のハンドシェイクだけで、以降はリクエストへのレスポンスしか返せない
  - X Protocol はレスポンスとは独立した Notice を持つため、警告や状態変化をやり取りの途中でも運べる
  - 参照:
    - [mysqlx-protocol-comparison.dox の比較表 (out-of-band notifications)](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/plugin/x/protocol/doc/mysqlx-protocol-comparison.dox#L38)
- 接続直後の手順
  - classic は接続直後の手順が「サーバー主導のハンドシェイクの中で capability 交換から認証まで一続きに行う」形に固定されている
  - X Protocol は接続直後の義務的な手続きがなく (サーバーは `ServerHello` の Notice を送るだけ)、capability ネゴシエーション (任意) も認証もクライアントが自分のタイミングで開始する
  - 参照:
    - [sql/auth/sql_authentication.cc の接続フェーズの doc コメント](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/sql/auth/sql_authentication.cc#L127-L190)
    - [mysqlx-protocol-lifecycle.dox の Stages of Session Setup](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/plugin/x/protocol/doc/mysqlx-protocol-lifecycle.dox#L118-L128)

## メッセージ構造 (フレーム)

- すべてのメッセージは以下のフレーム構造で送受信される
  - 参照:
    - [mysqlx.proto 冒頭の doc コメント `@section messages_Message_Structure`](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/plugin/x/protocol/protobuf/mysqlx.proto#L48-L70)

```txt
struct Message {
  uint32 length;                              // 4 バイト、リトルエンディアン
  uint8  message_type;                        // 1 バイト
  opaque message_payload[Message.length - 1]; // protobuf でエンコードされたペイロード
};
```

- フレームは送受信の単位で、下記の `length` + `message_type` + `message_payload` の 1 まとまり
  - length
    - `message_type` (1 バイト) と `message_payload` の長さの合計 (`length` フィールド自身の 4 バイトは含まない)
  - message_type
    - クライアント発なら `ClientMessages.Type`、サーバー発なら `ServerMessages.Type` の enum 値
  - message_payload
    - `message_type` に対応する protobuf メッセージをシリアライズしたバイト列
- TCP はメッセージの境界を持たない単なるバイトストリームであるため、先頭の `length` を使って受信側が「どこまでが 1 メッセージか」を切り出す
- フレームはあくまで論理的な区切りであり、TCP レイヤーのパケット分割とは対応しない
  - つまり 1 つのフレームが複数のパケットに分かれて届くことも、複数のフレームが 1 つのパケットにまとまって届くこともある
  - そのため受信側は `length` 分のバイトが揃うまで読み溜めてから 1 フレームとして切り出す

### 補足

- 1 つのフレームに入る protobuf メッセージは 1 個だけ
- protobuf のシリアライズ結果は型情報を含まないため、受信側は `message_type` の値を見てペイロードをどの型としてデシリアライズするかを決める
  - 種別値とメッセージ型の対応表は `.proto` ファイルに定義されていて、同じ定義から生成したクライアントとサーバーの双方に最初から埋め込まれている (一覧と宣言のされ方は [message_spec.md のメッセージ種別の一覧](./message_spec.md#メッセージ種別の一覧) を参照)

## 参考文献

- https://dev.mysql.com/doc/dev/mysql-server/8.4.11/page_mysqlx_protocol.html
