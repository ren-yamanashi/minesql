# パケット

## 参考文献

- [MySQL Source Code Documentation - Protocol Basics](https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_basics.html)

## パケットの形式

- サーバー・クライアントがデータを送信する際には、データを $2^{24}$ バイトごとのパケットに分割する
  - 16MB を超えるデータを送信する (ペイロードが $2^{24}-1$ バイトを超える) 場合、パケットの長さを $2^{24} -1$ バイト (`ff ff ff`) に設定し、残りのデータを次のパケットに分割して送信する
    - これをパケット長が $2^{24}-1$ 未満になるまで繰り返す
- 分割された各データ (チャンク) の先頭にパケットヘッダーを付与する

| サイズ | 名前 | 説明 |
| --- | --- | --- |
| 3 バイト | パケット長 | ペイロードの長さ |
| 1 バイト | シーケンス ID | パケットのシーケンス ID |
| 可変長 | ペイロード | パケットの内容 |

- シーケンス ID は、パケットごとに加算され、上限に達すると 0 に戻る
  - 初期値: 0
  - コマンドフェーズで新しいコマンドが開始されるたびに 0 にリセットされる

### 例

```txt
01 00 00 00 01
```

このパケットは以下のように解釈される

- パケット長: 1 バイト (0x01)
- シーケンス ID: 0 (0x00)
- ペイロード: 1 バイト (0x01)

## 接続フェーズのパケット

### 初期ハンドシェイクパケット

- サーバーがクライアントに送信する最初のパケット
- 参考: https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_connection_phase_packets_protocol_handshake_v10.html

| フィールド | サイズ | 説明 |
| --- | --- | --- |
| protocol_version | 1 バイト | プロトコルバージョン (常に 10) |
| server_version | NULL 終端文字列 | サーバーのバージョン文字列 |
| connection_id | 4 バイト | コネクション ID |
| auth_plugin_data_part_1 | 8 バイト | Nonce の前半 8 バイト |
| filler | 1 バイト | 常に 0x00 |
| capability_flags_lower | 2 バイト | Capability Flags の下位 2 バイト |
| character_set | 1 バイト | サーバーの文字セット |
| status_flags | 2 バイト | サーバーの状態フラグ |
| capability_flags_upper | 2 バイト | Capability Flags の上位 2 バイト |
| auth_plugin_data_len | 1 バイト | 認証データの全長 |
| reserved | 10 バイト | 予約領域 (全て 0x00) |
| auth_plugin_data_part_2 | 13 バイト | Nonce の後半 12 バイト + NUL 終端 |
| auth_plugin_name | NULL 終端文字列 | 認証プラグイン名 |

- Capability Flags を上位・下位に分割しているのは、MySQL 4.0 以前との後方互換性のため

### ハンドシェイク応答パケット

- クライアントがサーバーに送信するハンドシェイクの応答
- 参考: https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_connection_phase_packets_protocol_handshake_response.html

| フィールド | サイズ | 説明 |
| --- | --- | --- |
| capability_flags | 4 バイト | クライアントの Capability Flags |
| max_packet_size | 4 バイト | クライアントが受け入れる最大パケットサイズ |
| character_set | 1 バイト | クライアントの文字セット |
| reserved | 23 バイト | 予約領域 (全て 0x00) |
| username | NULL 終端文字列 | ユーザー名 |
| auth_response | 可変長 | CLIENT_SECURE_CONNECTION がある場合: 1 バイトの長さ + Scramble データ。ない場合: このフィールドは存在しない |
| database | NULL 終端文字列 | CLIENT_CONNECT_WITH_DB がある場合のみ存在。接続先のデータベース名 |
| auth_plugin_name | NULL 終端文字列 | CLIENT_PLUGIN_AUTH がある場合のみ存在。認証プラグイン名 |

- auth_response のフィールド構造はクライアントの Capability Flags によって決まる

### AuthMoreData パケット

- 認証フェーズの途中で、サーバーからクライアントに追加の認証情報を送信するためのパケット
- caching_sha2_password では、Fast Authentication の成功通知や Complete Authentication への切り替え通知に使用される

| フィールド | サイズ | 説明 |
| --- | --- | --- |
| ヘッダー | 1 バイト | 常に 0x01 |
| status_byte | 1 バイト | 0x03 = Fast Authentication 成功、0x04 = Complete Authentication に切り替え |

## 汎用レスポンスパケット

- クライアントから送られたほとんどのコマンドへのレスポンスとして、以下のいずれかのパケットを返す

### OK_Packet

- コマンドが正常に完了したことを知らせるためのパケット
- OK_Packet は EOF を示すためにも使用される (MySQL 5.7.5 以降で EOF_Packet は非推奨となっており、MineSQL では EOF_Packet を使用しない)
- 詳細: https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_basic_ok_packet.html

| フィールド | サイズ | 説明 |
| --- | --- | --- |
| ヘッダー | 1 バイト | 常に 0x00 |
| affected_rows | 長さエンコード整数 | 影響を受けた行数。INSERT は挿入行数、UPDATE は値が変更された行数、DELETE は削除行数。DDL やトランザクション制御では 0 |
| last_insert_id | 長さエンコード整数 | 最後に挿入された行の ID (MineSQL では常に 0) |
| status_flags | 2 バイト | サーバーの状態フラグ (トランザクション中か、autocommit かなど) |
| warnings | 2 バイト | 警告の数 (MineSQL では常に 0) |

- status_flags の値
  - `SERVER_STATUS_IN_TRANS` (0x0001): トランザクション実行中
  - `SERVER_STATUS_AUTOCOMMIT` (0x0002): autocommit モード

### ERR_Packet

- エラーが発生したことを通知するためのもの
- 詳細: https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_basic_err_packet.html

| フィールド | サイズ | 説明 |
| --- | --- | --- |
| ヘッダー | 1 バイト | 常に 0xFF |
| error_code | 2 バイト | エラーコード (例: 1064 = 構文エラー、1045 = アクセス拒否) |
| sql_state_marker | 1 バイト | 常に '#' |
| sql_state | 5 バイト | SQL 状態コード (後述) |
| error_message | 可変長 | エラーメッセージ (パケット末尾まで) |

- SQL State
  - SQL 標準 (ISO/IEC 9075) で定義された 5 文字のステータスコード
  - 先頭 2 文字がクラス (エラーの大分類)、残り 3 文字がサブクラス (詳細分類) を表す
    - 例: クラス `42` は「構文エラーまたはアクセスルール違反」を示す。サブクラスでさらに原因を細分化する
      - `42000`: サブクラス `000` = 汎用 (具体的な分類なし)
      - `42S02`: サブクラス `S02` = テーブルまたはビューが存在しない
  - MySQL はこの仕様に準拠しつつ、独自のエラーコードと組み合わせて使用している
    - エラーコード (error_code) は MySQL 固有の整数値
    - SQL State は標準に基づく分類コード
    - 同じエラーコードには常に同じ SQL State が対応する

MineSQL が使用するエラーコードと対応する SQL State

| エラーコード | SQL State | 意味 |
| --- | --- | --- |
| 1045 | 28000 | 認証失敗 (ユーザー名またはパスワードの不一致) |
| 1064 | 42000 | SQL 構文エラー |
| 1105 | HY000 | 汎用エラー (上記に該当しないエラー) |

SQL State のクラス一覧 (上記で使用されるもの)

| クラス | 意味 |
| --- | --- |
| 28 | 認可に関する異常 (Invalid Authorization Specification) |
| 42 | 構文エラーまたはアクセスルール違反 (Syntax Error or Access Rule Violation) |
| HY | 固有クラスなし (No Specific SQLSTATE Class)。特定のクラスに分類できない汎用的なエラーに使用される |

## ペイロードで使用される型

パケットのペイロードは以下の型で構成される。

### Integer

- 固定長整数型
  - 固定長符号なし整数は、一連のバイト列として、最下位バイトから順に格納される (LittleEndian)

- 長さエンコード整数型
  - 数値に応じて、1 バイト、3 バイト、4 バイト、または 9 バイトを消費する整数

| 以上 | 未満 | 保存 |
| --- | --- | --- |
| 0 | 251 | 1 バイト整数 |
| 251 | $2^{16}$ | 0xFC + 2 バイト整数 |
| $2^{16}$ | $2^{24}$ | 0xFD + 3 バイト整数 |
| $2^{24}$ | $2^{64}$ | 0xFE + 8 バイト整数 |

同様に、長さエンコードされた整数を数値に変換する際には、最初のバイトを読み取って、整数がどのようにエンコードされているかを判断する

### String

- 固定長文字列型
  - あらかじめ決められた (ハードコードされた) 長さを持つ文字列
  - 例: ERR_Packet の sql_state は常に 5 バイトの文字列

- NULL 終端文字列型
  - NULL 文字 (0x00) で終端される文字列

- 可変長文字列型
  - 文字列の長さは、別のフィールドによって決定されるか、実行時に計算される

- 長さ指定付き文字列型
  - 文字列の長さを示す「長さエンコード整数」を先頭に付加した文字列
  - 可変長文字列型の一種 (特殊ケース)

- パケットの残りの文字列
  - 文字列がパケットの最後の構成要素である場合、その長さはパケットの長さから現在の位置を差し引くことで算出できる
