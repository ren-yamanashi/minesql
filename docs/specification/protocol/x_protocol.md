# X Protocol

## 概要

- MySQL の X Protocol は MySQL 5.7.12 で導入された通信プロトコル
- X Protocol は Protocol Buffers でデータをシリアライズする
  - メッセージの構造を `.proto` ファイルで定義してバイナリで運ぶので、各言語のコネクタを同じ定義から生成できる
  - 型の定義は https://github.com/mysql/mysql-server/tree/aa461240270d809bcac336483b886b3d1789d4d9/plugin/x/protocol/protobuf

### Protocol Buffers (protobuf)

- Protocol Buffers (protobuf) は、Google が開発した軽量で効率的なデータシリアライズフォーマット
- JSON や XML と同じく、データの構造を定義して、異なるシステムの間でデータを交換するためのもの
- バイナリ形式なので、JSON や XML よりもデータサイズが小さく高速
- `.proto` ファイルに型安全にデータ構造を定義できる

## X Protocol / X Plugin / X DevAPI の関係

X Protocol の周辺には名前の似た用語が 3 つあり、それぞれ層が違う

- X Protocol: 通信規約
  - TCP または Unix ソケット上を流れるフレーム + protobuf メッセージの規約
- X Plugin: サーバー側の実装
  - X Protocol を待ち受けるサーバー側エンドポイントで、接続受付・capability・認証 などのプロトコル処理全般を担う
  - 素の SQL 実行に加えて、MySQL をドキュメントストアとして使うためのドキュメントモデルインターフェースを提供する
    - CRUD メッセージ (`Mysqlx.Crud.Find` / `Insert` / `Update` / `Delete`) や `create_collection` などの管理コマンドを、JSON カラムを持つ通常の InnoDB テーブルに対する SQL に変換して実行する
    - JSON 型と JSON 関数は MySQL サーバーが元々持つ機能で、X Plugin が足すのは SQL を書かずにそれらを操作するためのメッセージだけ
- X DevAPI: クライアント側の API 仕様
  - MySQL Shell や各言語の Connector が実装する API で、内部で X Protocol を使って通信する
  - コレクションもリレーショナルテーブルも、SQL を書かずにメソッドチェーンの CRUD で操作できる
    - コレクションへの操作は、上の CRUD メッセージとしてサーバーに送られる
  - X DevAPI を使わずに、`.proto` からコードを生成して X Protocol で直接通信することもできる

## 主な特徴

- レスポンスを待たずに次のリクエストを送れるので、往復の待ち時間を減らせる
- 再接続せずに同じ接続の上でセッションをリセットして使い回せるので、接続の確立にかかる時間を減らせる
- 新しい機能やフィールドをクライアントが無視できるので、古いクライアントを壊さずにサーバーを拡張できる

## classic protocol (Client/Server プロトコル) との違い

classic protocol での課題と、その課題を X Protocol がどう解消したかについて記載

### メッセージの解釈

- classic protocol の場合は、同じレスポンスのバイト列でも直前に送ったコマンドによってその意味が変わるので、クライアントが読み分けを手で実装する必要があった
- X Protocol は、メッセージの先頭に種別が付いていて、種別ごとの読み方は `.proto` から生成したコードで定義されているので、クライアントは先頭の種別を見れば読み方が分かる

### リクエストとレスポンスの対応付け

- classic protocol は、1 コマンド送った後レスポンスを受け取ってから次のコマンドを送る前提で設計されているので、コマンドのたびに往復の待ちが発生する
- X Protocol では、どのリクエストもレスポンスの最後に来るメッセージが決まっていて (`StmtExecute` なら `StmtExecuteOk` か `Error`)、レスポンスはリクエストの順に返るので、クライアントはレスポンスを待たずに次のリクエストを送っても、返ってきたレスポンスをリクエストごとに区切れる
  - レスポンスを待たずに次のリクエストを送れるので往復の待ち時間を減らせる

### capability の表現

- classic protocol は、使える機能を 32 個のオン / オフのビットで伝えている
  - オン / オフしか表せないので、圧縮レベルのような値は、ビットが立っているときだけ付く追加の項目で別に運んでいたりする
  - 32 個はほぼ使い切っていて、1 つは 64 ビットに広げるための予約になっている
- X Protocol は、機能を「名前と値」のセットで伝える
  - 値には数値、文字列、構造を持つデータをそのまま入れられ、新しい機能は名前を 1 つ足せば増やせる

### サーバー起点の通知

- classic protocol では、サーバーが自分からメッセージを送るのは接続直後のハンドシェイクだけで、以降はリクエストへのレスポンスしか返せない
- X Protocol はサーバーから任意のタイミングで警告や状態変化などの通知 (Notice) を送れる

### 接続直後の手順

- classic protocol は、サーバーが先に送るハンドシェイクパケットの中で capability の交換と認証を一続きに行うので、手順と順番がパケットの形式で決まっている
  - どのクライアントも同じ手順を踏み、手順を変えるにはパケットの形式を変えることになる
- X Protocol で決められている手順は「サーバーが接続直後に `ServerHello` の Notice を送る」ことだけで、その後の決まった手続きはない
  - capability の取得、TLS や圧縮の設定、認証はそれぞれ独立したメッセージで、クライアントは要るものだけを送れる
  - 新しい手順を足すときも、送るメッセージが 1 つ増えるだけで、ハンドシェイクの形式は変わらない

## メッセージ構造 (フレーム)

すべてのメッセージは以下の「フレーム」という構造で送受信される

```txt
struct Message {
  uint32 length;                              // 4 バイト、リトルエンディアン
  uint8  message_type;                        // 1 バイト
  opaque message_payload[Message.length - 1]; // protobuf でエンコードされたペイロード
};
```

- フレームは送受信の単位で、`length` + `message_type` + `message_payload` の 1 まとまり
  - length
    - `message_type` (1 バイト) と `message_payload` の長さの合計 (`length` フィールド自身のバイトは含まない)
  - message_type
    - クライアント発なら `ClientMessages.Type`、サーバー発なら `ServerMessages.Type` の enum 値
  - message_payload
    - `message_type` に対応する protobuf メッセージをシリアライズしたバイト列
- TCP はメッセージの境界を持たないバイトストリームなので、受信側は先頭の `length` で 1 メッセージの終わりを知る

### 補足

- `フレームの区切り = TCP のパケットの区切り` ではない
  - 1 つのフレームが複数のパケットに分かれて届くことも、複数のフレームが 1 つのパケットで届くこともある
  - 受信側は `length` 分のバイトが揃うまで読み溜めてから 1 フレームとして取り出す
- 1 つのフレームに入る protobuf メッセージは 1 個だけ
- protobuf のシリアライズ結果は型情報を含まないので、受信側は `message_type` の値でペイロードの型を決める
  - `message_type` の値に対応する実際の型定義は `.proto` ファイルに定義されている

## 参考資料

- [MySQL Server Doxygen: X Protocol (8.4.11)](https://dev.mysql.com/doc/dev/mysql-server/8.4.11/page_mysqlx_protocol.html)
- [sql-common/net_serv.cc のパケット構造の doc コメント](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/sql-common/net_serv.cc)
- [sql/protocol_classic.cc のコマンドフェーズの doc コメント](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/sql/protocol_classic.cc)
- [mysqlx.proto の `@section messages_Message_Sequence`](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/plugin/x/protocol/protobuf/mysqlx.proto)
- [mysqlx-protocol-implementation.dox の Pipelining](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/plugin/x/protocol/doc/mysqlx-protocol-implementation.dox)
- [include/mysql_com.h の zstd 圧縮フラグ](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/include/mysql_com.h)
- [sql/auth/sql_authentication.cc の zstd レベルの追加フィールド](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/sql/auth/sql_authentication.cc)
- [mysqlx-protocol-comparison.dox の比較表 (out-of-band notifications)](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/plugin/x/protocol/doc/mysqlx-protocol-comparison.dox)
- [mysqlx-protocol-lifecycle.dox の Stages of Session Setup](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/plugin/x/protocol/doc/mysqlx-protocol-lifecycle.dox)
