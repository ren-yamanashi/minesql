# X Protocol のメッセージ仕様

- X Protocol の各メッセージの詳細仕様
- ここに書くのは `.proto` ファイルが定める契約 (メッセージの定義、値の型、エンコーディング、種別の一覧) で、minesql はこれにそのまま従う
- X Plugin の振る舞いの細部 (実測値、doc コメントと実装の差、状態ごとの例外的な応答) は [reference/x_plugin_behavior.md](./reference/x_plugin_behavior.md) に分けた
  - 契約ではないが、クライアントはこの振る舞いも前提にしうるため、minesql も互換の要件として同じ応答をする

## 型定義ファイルの構成

| ファイル | package | 定義内容 |
| --- | --- | --- |
| [mysqlx.proto](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/plugin/x/protocol/protobuf/mysqlx.proto) | `Mysqlx` | フレーム構造とメッセージシーケンスの規則 (doc コメント)、メッセージ種別の enum (`ClientMessages` / `ServerMessages`)、汎用の `Ok` / `Error` |
| [mysqlx_connection.proto](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/plugin/x/protocol/protobuf/mysqlx_connection.proto) | `Mysqlx.Connection` | capability ネゴシエーション (`CapabilitiesGet` / `CapabilitiesSet` / `Capabilities`)、接続の終了 (`Close`)、圧縮 (`Compression`) |
| [mysqlx_session.proto](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/plugin/x/protocol/protobuf/mysqlx_session.proto) | `Mysqlx.Session` | 認証 (`AuthenticateStart` / `AuthenticateContinue` / `AuthenticateOk`)、セッションのリセット (`Reset`) と終了 (`Close`) |
| [mysqlx_sql.proto](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/plugin/x/protocol/protobuf/mysqlx_sql.proto) | `Mysqlx.Sql` | SQL ステートメントの実行 (`StmtExecute` / `StmtExecuteOk`) |
| [mysqlx_resultset.proto](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/plugin/x/protocol/protobuf/mysqlx_resultset.proto) | `Mysqlx.Resultset` | リザルトセット (`ColumnMetaData` / `Row` / `FetchDone` 系) と行データのエンコーディング (doc コメント) |
| [mysqlx_datatypes.proto](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/plugin/x/protocol/protobuf/mysqlx_datatypes.proto) | `Mysqlx.Datatypes` | 汎用データ型 (`Scalar` / `Object` / `Array` / `Any`) |
| [mysqlx_notice.proto](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/plugin/x/protocol/protobuf/mysqlx_notice.proto) | `Mysqlx.Notice` | Notice (`Frame` と 5 種類のペイロード) |
| [mysqlx_expect.proto](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/plugin/x/protocol/protobuf/mysqlx_expect.proto) | `Mysqlx.Expect` | Expect ブロック (`Open` / `Close`) |
| [mysqlx_crud.proto](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/plugin/x/protocol/protobuf/mysqlx_crud.proto) | `Mysqlx.Crud` | CRUD 操作 (`Find` / `Insert` / `Update` / `Delete`、ビュー操作) |
| [mysqlx_expr.proto](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/plugin/x/protocol/protobuf/mysqlx_expr.proto) | `Mysqlx.Expr` | 式の Tree (CRUD のフィルタ条件や射影で使う) |
| [mysqlx_prepare.proto](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/plugin/x/protocol/protobuf/mysqlx_prepare.proto) | `Mysqlx.Prepare` | プリペアドステートメント (`Prepare` / `Execute` / `Deallocate`) |
| [mysqlx_cursor.proto](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/plugin/x/protocol/protobuf/mysqlx_cursor.proto) | `Mysqlx.Cursor` | カーソル (`Open` / `Fetch` / `Close`) |

- 下 4 つ (crud / expr / prepare / cursor) のメッセージは minesql では実装しない ([ideology.md](../ideology.md) の「目的とスコープ」)
- `.proto` ファイルは proto2 記法で書かれており、protobuf 3 系のツールで扱う場合も 2.x の規則が適用される

## 汎用データ型 (Mysqlx.Datatypes)

- 参照:
  - [mysqlx_datatypes.proto](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/plugin/x/protocol/protobuf/mysqlx_datatypes.proto)
- protobuf のフィールドは型を静的に宣言する必要があるため、「送るまで型が決まらない値」をそのままでは表現できない
- そこで、SQL 実行のパラメータ (`StmtExecute.args`) や capability の値 (`Capability.value`)、Notice が運ぶ値のように任意の値を渡す場面のために、汎用のコンテナ型が定義されている
- 構造は JSON と同じ発想で、以下の 3 種を入れ子にして組み合わせられる
  - `Scalar`: 単一値 (整数、浮動小数点数、bool、文字列、バイト列、NULL)
    - `.proto` ファイル上の種別は `V_SINT` / `V_UINT` / `V_NULL` / `V_OCTETS` / `V_DOUBLE` / `V_FLOAT` / `V_BOOL` / `V_STRING` の 8 種
    - `V_STRING` は collation 付きの文字列、`V_OCTETS` は content_type 付きのバイト列
  - `Object`: キーと値 (`Any`) の組のリスト (JSON のオブジェクトに相当)
  - `Array`: 値 (`Any`) のリスト (JSON の配列に相当)
- `Any` は `Scalar` / `Object` / `Array` のいずれか 1 つを保持する共用体
  - 任意の値を受け取るフィールドは、この `Any` 型 (`StmtExecute.args` や `Capability.value` など) か、単一値で足りる場合は `Scalar` 型 (Notice が運ぶ値など) で宣言されている
- 例として、capability の値 (`Capability.value`) では以下のように使い分けられている
  - `tls` や `client.interactive` のような単一の bool 値は `SCALAR`
  - `authentication.mechanisms` は文字列のリストなので `ARRAY`
  - `compression` は `algorithm` というキーを持つので `OBJECT` (その値はさらに `ARRAY`)
  - 実際のレスポンス全体は [reference/x_plugin_behavior.md の実測値](./reference/x_plugin_behavior.md#capability-の実測値) を参照

## capability ネゴシエーション

- 参照:
  - [mysqlx_connection.proto](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/plugin/x/protocol/protobuf/mysqlx_connection.proto)
- `CapabilitiesGet`: サーバーが持つ capability とその現在値の一覧 (`Capabilities`) を取得する
- `CapabilitiesSet`: capability の変更をリクエストし、`Ok` または `Error` が返る (例: `tls: true` を送って TLS 接続へ切り替える)
  - 変更は認証前にしか行えず、まとめて適用される (1 つでも失敗するとすべての変更が破棄される)
  - 存在しない名前を指定すると `Error` (`Capability '<名前>' doesn't exist`) になる
  - 参照:
    - [CapabilitiesSet の前提条件 (doc コメント)](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/plugin/x/protocol/protobuf/mysqlx_connection.proto#L74)
    - [configurator.cc の存在しない名前の扱い](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/plugin/x/src/capabilities/configurator.cc#L91-L92)
- `Capabilities` は `Capability` (`name` 文字列 + `Mysqlx.Datatypes.Any` の値) のリスト
- MySQL 8.4 の X Plugin が持つ capability は 8 個
  - 参照:
    - [client.cc の capability の登録](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/plugin/x/src/client.cc#L202-L218)

| 名前 | 値の型 | 設定 | 意味 |
| --- | --- | --- | --- |
| `tls` | bool | 可 | `true` を設定すると接続を TLS に切り替える |
| `authentication.mechanisms` | 文字列の `Array` | 不可 | この接続で使える認証メカニズムの一覧 (安全な接続かどうかで変わる) |
| `doc.formats` | 文字列 | 不可 | 固定値 `"text"` |
| `client.interactive` | bool | 可 | クライアントが対話型かどうかをサーバーに伝える |
| `session_connect_attrs` | `Object` | 設定のみ (取得不可) | 接続属性 (名前と値の組) |
| `compression` | `Object` | 可 | 取得時は使える圧縮アルゴリズムの一覧、設定時は使うアルゴリズムの指定 |
| `node_type` | 文字列 | 不可 | 固定値 `"mysql"` |
| `client.pwd_expire_ok` | bool | 可 | 期限切れパスワードのアカウントでの接続をクライアントが扱えるかどうか |

## 認証 (セッション確立)

- 参照:
  - [mysqlx_session.proto](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/plugin/x/protocol/protobuf/mysqlx_session.proto)
- `AuthenticateStart` で使いたい認証メカニズム (`mech_name`) を指定して認証を開始する
  - フィールドは `mech_name` (必須) / `auth_data` / `initial_response` の 3 つ
  - プロトコルとしての既定のメカニズムはなく、クライアントが必ず明示する (公式クライアントの自動選択の順序は [reference/x_plugin_behavior.md](./reference/x_plugin_behavior.md#公式クライアントの認証メカニズムの自動選択) を参照)
- メカニズムによっては `AuthenticateContinue` の往復で追加の認証データを交換する
- 成功なら `AuthenticateOk`、失敗なら `Error` が返る
- `mech_name` は `.proto` ファイル上は自由な文字列で、使えるメカニズムはサーバー実装側で決まる
  - MySQL 8.4 の X Plugin が登録するメカニズムは `MYSQL41`・`PLAIN`・`SHA256_MEMORY` の 3 種類で、`PLAIN` は安全な接続 (TLS または Unix ソケット) でのみ使える
    - `authentication.mechanisms` capability の一覧も安全な接続かどうかで切り替わる (安全でなければ `MYSQL41` と `SHA256_MEMORY` のみ)
    - 参照:
      - [authentication_container.cc のメカニズム登録](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/plugin/x/src/server/authentication_container.cc#L37-L46)
      - [get_auth_handler / get_authentication_mechanisms](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/plugin/x/src/server/authentication_container.cc#L49-L80)
      - [connection_type.cc の is_secure_type](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/plugin/x/src/io/connection_type.cc#L58-L66)
  - 例: `MYSQL41` はチャレンジレスポンス方式で、サーバーが `AuthenticateContinue` で送る 20 バイトの salt とパスワードから計算したレスポンスを返す
    - 参照:
      - [auth_challenge_response.h の doc コメント](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/plugin/x/src/auth_challenge_response.h#L55-L60)
      - [challenge_response_verification.cc の generate_salt](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/plugin/x/src/challenge_response_verification.cc#L41-L45)

## セッションと接続の終了・リセット

- 参照:
  - [mysqlx_session.proto](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/plugin/x/protocol/protobuf/mysqlx_session.proto)
  - [mysqlx_connection.proto](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/plugin/x/protocol/protobuf/mysqlx_connection.proto)
- `Session.Reset` はセッションの状態をリセットする
  - `keep_open = true` なら認証済みのままセッションだけをリセットする
  - `keep_open = false` (既定) ならセッションを閉じ、再認証が必要になる
- `Session.Close` は現在のセッションを閉じて `Ok` を返し、接続は再認証待ちに戻る
  - 再認証待ちの接続が受け付けるのは認証のメッセージだけで、それ以外への応答は [reference/x_plugin_behavior.md](./reference/x_plugin_behavior.md#再認証待ちの接続が受け付けるメッセージ) を参照
- `Connection.Close` は接続そのものを閉じる意思をサーバーへ伝える
  - サーバーはセッションの状態を破棄し、`Ok` ("bye!") を返してから接続を切断する

## SQL 実行 (Sql.StmtExecute)

- 参照:
  - [mysqlx_sql.proto](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/plugin/x/protocol/protobuf/mysqlx_sql.proto)

| フィールド | 型 | 説明 |
| --- | --- | --- |
| `namespace` | `string` (既定 `"sql"`) | ステートメントを実行する namespace |
| `stmt` | `bytes` (必須) | 実行するステートメント |
| `args` | `Mysqlx.Datatypes.Any` の repeated | ステートメント中のプレースホルダ (`?`) を置き換える値 |
| `compact_metadata` | `bool` (既定 `false`) | `true` なら `ColumnMetaData` を型情報 (`type`) のみに省略する |

- `namespace` には `"sql"` (SQL ステートメントの実行) のほかに `"mysqlx"` (管理コマンドの実行) がある
  - `"mysqlx"` では `stmt` にコマンド名を指定し、`args` には名前付き引数を持つ `Object` を 1 つ渡す
  - コマンドは 14 個 (`ping` / `list_clients` / `kill_client` / `create_collection` / `drop_collection` / `ensure_collection` / `modify_collection_options` / `get_collection_options` / `create_collection_index` / `drop_collection_index` / `list_objects` / `enable_notices` / `disable_notices` / `list_notices`) で、minesql が実装するのはコレクション系を除く 6 個 ([dispatcher/](../dispatcher/README.md))
  - 参照:
    - [mysqlx-protocol-xplugin.dox の namespace の説明](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/plugin/x/protocol/doc/mysqlx-protocol-xplugin.dox#L36-L51)
    - [admin_cmd_handler.cc のコマンド表](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/plugin/x/src/admin_cmd_handler.cc#L96-L115)

### リザルトセットの構造

- 参照:
  - [mysqlx_resultset.proto](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/plugin/x/protocol/protobuf/mysqlx_resultset.proto) (メッセージ列の規則は同ファイル冒頭の doc コメント)
- 1 つのリザルトセットは 1 個以上の `ColumnMetaData` と 0 個以上の `Row` からなる
- 各リザルトセットの後には以下のいずれかが続く
  - `FetchDoneMoreResultsets`: さらに別のリザルトセットが続く (複数のリザルトセットを返す `CALL` など)
  - `FetchDoneMoreOutParams`: OUT パラメータのリザルトセットが続く
  - `FetchDone`: 最後のリザルトセットが送信済み
- `FetchSuspended` はカーソル使用時にリザルトセットの送信を中断した状態を表す
- リザルトセットを返さないステートメント (`INSERT` など) では `ColumnMetaData` / `Row` / `FetchDone` は送られず、Notice と `StmtExecuteOk` だけが返る
  - doc コメントの記述とは異なる (詳細は [reference/x_plugin_behavior.md](./reference/x_plugin_behavior.md#doc-コメントと実装の差))
- `SELECT 1 LIMIT 0` のような空のリザルトセットは `ColumnMetaData` + `FetchDone` になる

### 実行ステータスの Notice

- Affected Rows (`ROWS_AFFECTED`) や Last Insert ID (`GENERATED_INSERT_ID`) は、リザルトセットとは別に Notice (`SessionStateChanged`) として届く (順序は [communication_flow.md の SQL 実行の流れ](./communication_flow.md#sql-実行の流れ))

## リザルトセットのエンコーディング

### ColumnMetaData

- 参照:
  - [mysqlx_resultset.proto](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/plugin/x/protocol/protobuf/mysqlx_resultset.proto) の `ColumnMetaData` (エンコーディングの説明は直前の doc コメント)
- フィールドは `type` (必須)、`name`、`original_name`、`table`、`original_table`、`schema`、`catalog`、`collation`、`fractional_digits`、`length`、`flags`、`content_type`
- `original_name` / `original_table` は別名を付ける前の名前で、素の名前と同じ場合サーバーは省略してよい (クライアント側で補完する)
- `catalog` は MySQL にカタログの概念がないため意味を持たず、MySQL は固定値 `"def"` を入れて送る (`compact_metadata` でない場合)
  - 参照:
    - [streaming_command_delegate.cc の field_metadata](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/plugin/x/src/streaming_command_delegate.cc#L309-L313)
- `flags` は全型共通のビット (`NOT_NULL` 0x0010、`PRIMARY_KEY` 0x0020、`UNIQUE_KEY` 0x0040、`MULTIPLE_KEY` 0x0080、`AUTO_INCREMENT` 0x0100) と型別のビット (いずれも 0x0001) を持つ
  - 型別のビットは `UINT` の zerofill、`DOUBLE` / `FLOAT` / `DECIMAL` の unsigned、`BYTES` の rightpad、`DATETIME` の is_timestamp
- `content_type` は `BYTES` 型の中身のヒント (`GEOMETRY` = 1、`JSON` = 2、`XML` = 3) と `DATETIME` 型の中身のヒント (`DATE` = 1、`DATETIME` = 2) を表す (同ファイルの `ContentType_BYTES` / `ContentType_DATETIME`)
- `compact_metadata` が指定された場合は `type` のみが設定される

### Row

- 参照:
  - [mysqlx_resultset.proto](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/plugin/x/protocol/protobuf/mysqlx_resultset.proto) の `Row`
- 行は `repeated bytes field`、つまりフィールドごとのバイト列のリストとして表現される
- 各フィールドのエンコーディングは対応する `ColumnMetaData.type` (`FieldType`) で決まる
- SQL の NULL 値は長さ 0 のフィールドとして送られる

| SQL 型 | FieldType | 値のエンコード |
| --- | --- | --- |
| TINYINT 〜 BIGINT | `SINT` (1) | 可変長エンコードされた符号付き 64 ビット整数 |
| TINYINT 〜 BIGINT の UNSIGNED、YEAR | `UINT` (2) | 可変長エンコードされた符号なし 64 ビット整数 |
| DOUBLE | `DOUBLE` (5) | protobuf の `double` |
| FLOAT | `FLOAT` (6) | protobuf の `float` |
| CHAR / VARCHAR / TEXT / BLOB / BINARY / GEOMETRY など | `BYTES` (7) | バイト列の末尾に `0x00` を 1 バイト付加したもの |
| TIME | `TIME` (10) | 符号 1 バイト + 時・分・秒・マイクロ秒の可変長整数 |
| DATE / DATETIME / TIMESTAMP | `DATETIME` (12) | 年・月・日 (+ 時・分・秒・マイクロ秒) の可変長整数の列 |
| SET | `SET` (15) | 長さを前置したバイト列の並び |
| ENUM | `ENUM` (16) | `BYTES` と同じ形式 |
| BIT | `BIT` (17) | 可変長エンコードされた符号なし 64 ビット整数 |
| DECIMAL | `DECIMAL` (18) | scale 1 バイト + packed BCD + 符号ニブル |

- `BYTES` の末尾 `0x00` は、NULL (長さ 0 のフィールド) と空文字列 (長さ 1 のフィールド) を区別するためのもの
- `TIME` / `DATETIME` の下位要素は、右側がすべて 0 の場合省略できる (例: `0x00` だけなら `+00:00:00.000000`)
- 実測例: `SELECT 1 AS id, 'Alice' AS name` の `Row` ペイロードは `0a 01 02 0a 06 41 6c 69 63 65 00` の 11 バイト
  - `0a 01 02`: 1 つ目のフィールド (長さ 1) で、`SINT` なので zigzag デコードして `0x02` = 1
  - `0a 06 41 6c 69 63 65 00`: 2 つ目のフィールド (長さ 6) で、"Alice" の 5 バイト + 終端 `0x00`

## エラー (Mysqlx.Error)

- 参照:
  - [mysqlx.proto](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/plugin/x/protocol/protobuf/mysqlx.proto) の `Error`
- フィールドは `severity` / `code` (エラー番号) / `sql_state` (SQLSTATE) / `msg` (可読メッセージ)
- `severity = ERROR` は現在のメッセージシーケンスの中断を意味し、セッションは継続する
- `severity = FATAL` の場合、クライアントはサーバーが以降のメッセージを処理することを期待せず、接続を閉じるべきである

## Notice

- 参照:
  - [mysqlx_notice.proto](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/plugin/x/protocol/protobuf/mysqlx_notice.proto)
- Notice は共通の外枠 `Frame` (`type` / `scope` / `payload`) で送られ、`payload` に `type` に応じたメッセージが入る
- `scope` は `GLOBAL` (既定) と `LOCAL` の 2 種類
  - `LOCAL` は現在実行中のメッセージシーケンスに属する Notice
  - `GLOBAL` はシーケンスと無関係な Notice で、サーバーが自発的に送るのはこの Notice だけ
- アクティブなシーケンスがないときに `LOCAL` の Notice を受け取った場合、クライアントは無視すべきである

| `Frame.type` | ペイロードの型 | 内容 |
| --- | --- | --- |
| 1 | `Warning` | 警告と注意 (`level` / `code` / `msg`、LOCAL スコープでは `SHOW WARNINGS` の内容に対応) |
| 2 | `SessionVariableChanged` | セッション変数の変更 (`param` + `Scalar` 値) |
| 3 | `SessionStateChanged` | セッション内部状態の変化 (`param` + `Scalar` 値のリスト) |
| 4 | `GroupReplicationStateChanged` | グループレプリケーションの状態変化 |
| 5 | `ServerHello` | 接続を受け付けたことを伝える Notice |

- `SessionStateChanged.param` の主な値: `CURRENT_SCHEMA` (1)、`ACCOUNT_EXPIRED` (2)、`GENERATED_INSERT_ID` (3)、`ROWS_AFFECTED` (4)、`ROWS_FOUND` (5)、`ROWS_MATCHED` (6)、`TRX_COMMITTED` (7)、`TRX_ROLLEDBACK` (9)、`PRODUCED_MESSAGE` (10)、`CLIENT_ID_ASSIGNED` (11)、`GENERATED_DOCUMENT_IDS` (12)
- `ServerHello` は接続を受け付けた直後にサーバーが送る (システム変数 `mysqlx_enable_hello_notice` で制御され、既定で有効)

## Expect ブロック (Mysqlx.Expect)

- 参照:
  - [mysqlx_expect.proto](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/plugin/x/protocol/protobuf/mysqlx_expect.proto)
- 複数のメッセージをパイプラインで送る際の実行条件を `Open` / `Close` で囲んで指定する
- 条件 (`EXPECT_NO_ERROR` など) を満たさなくなると、ブロック内の後続のメッセージはすべて `Error` で失敗する
- 振る舞いの詳細は [dispatcher/](../dispatcher/README.md) を参照

## minesql で実装しない機能

- CRUD (mysqlx_crud.proto + mysqlx_expr.proto): SQL ステートメントを経由せずに `Find` / `Insert` / `Update` / `Delete` とビュー操作を直接表現する X DevAPI 用のメッセージ群
- プリペアドステートメント (mysqlx_prepare.proto): クライアントが採番した `stmt_id` で `Prepare` / `Execute` / `Deallocate` を行う
- カーソル (mysqlx_cursor.proto): プリペアドステートメントのリザルトセットを `Open` / `Fetch` で分割取得する
- 圧縮 (mysqlx_connection.proto の `Compression`): 圧縮したメッセージ列を運ぶコンテナで、クライアント (種別 46)・サーバー (種別 19) の双方向で使い、アルゴリズムは認証前に capability `compression` で合意する

## メッセージ種別の一覧

- `ClientMessages` / `ServerMessages` は「種別値とメッセージ型の対応表」を enum として定義したもの (protoc に定数生成と ID の一意性チェックをさせるための定義) で、通信路上を流れるデータではない
  - クライアントとサーバーは同じ `.proto` ファイルからコードを生成してビルドされるため、この対応表は最初から双方のプログラムに埋め込まれている (通信で共有する必要がなく、接続時に 1 度送られる、といったこともない)
  - 実行時に通信路上を流れるのは、各フレームの先頭に書き込まれる 1 バイトの種別値のみ
- この対応表は `.proto` ファイル内では 2 段階で宣言されている
  - [mysqlx.proto](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/plugin/x/protocol/protobuf/mysqlx.proto) の enum が種別値と種別名を対応付けている (例: `CON_CAPABILITIES_GET = 1;`)
  - 各メッセージ定義の末尾にある `option (client_message_id)` / `option (server_message_id)` が、そのメッセージ型と種別名を対応付けている (例: `message CapabilitiesGet { option (client_message_id) = CON_CAPABILITIES_GET; }`)
  - この 2 つをつなぐことで「`CapabilitiesGet` ⇔ 種別値 1」という対応が得られる

### クライアント → サーバー (mysqlx.proto の `ClientMessages.Type`)

| 値 | 種別名 | ペイロードの型 | 定義ファイル |
| --- | --- | --- | --- |
| 1 | `CON_CAPABILITIES_GET` | `Mysqlx.Connection.CapabilitiesGet` | mysqlx_connection.proto |
| 2 | `CON_CAPABILITIES_SET` | `Mysqlx.Connection.CapabilitiesSet` | mysqlx_connection.proto |
| 3 | `CON_CLOSE` | `Mysqlx.Connection.Close` | mysqlx_connection.proto |
| 4 | `SESS_AUTHENTICATE_START` | `Mysqlx.Session.AuthenticateStart` | mysqlx_session.proto |
| 5 | `SESS_AUTHENTICATE_CONTINUE` | `Mysqlx.Session.AuthenticateContinue` | mysqlx_session.proto |
| 6 | `SESS_RESET` | `Mysqlx.Session.Reset` | mysqlx_session.proto |
| 7 | `SESS_CLOSE` | `Mysqlx.Session.Close` | mysqlx_session.proto |
| 12 | `SQL_STMT_EXECUTE` | `Mysqlx.Sql.StmtExecute` | mysqlx_sql.proto |
| 17〜20 | `CRUD_FIND` / `CRUD_INSERT` / `CRUD_UPDATE` / `CRUD_DELETE` | `Mysqlx.Crud.Find` / `Insert` / `Update` / `Delete` | mysqlx_crud.proto |
| 24, 25 | `EXPECT_OPEN` / `EXPECT_CLOSE` | `Mysqlx.Expect.Open` / `Close` | mysqlx_expect.proto |
| 30〜32 | `CRUD_CREATE_VIEW` / `CRUD_MODIFY_VIEW` / `CRUD_DROP_VIEW` | `Mysqlx.Crud.CreateView` / `ModifyView` / `DropView` | mysqlx_crud.proto |
| 40〜42 | `PREPARE_PREPARE` / `PREPARE_EXECUTE` / `PREPARE_DEALLOCATE` | `Mysqlx.Prepare.Prepare` / `Execute` / `Deallocate` | mysqlx_prepare.proto |
| 43〜45 | `CURSOR_OPEN` / `CURSOR_CLOSE` / `CURSOR_FETCH` | `Mysqlx.Cursor.Open` / `Close` / `Fetch` | mysqlx_cursor.proto |
| 46 | `COMPRESSION` | `Mysqlx.Connection.Compression` | mysqlx_connection.proto |

### サーバー → クライアント (mysqlx.proto の `ServerMessages.Type`)

| 値 | 種別名 | ペイロードの型 | 定義ファイル |
| --- | --- | --- | --- |
| 0 | `OK` | `Mysqlx.Ok` | mysqlx.proto |
| 1 | `ERROR` | `Mysqlx.Error` | mysqlx.proto |
| 2 | `CONN_CAPABILITIES` | `Mysqlx.Connection.Capabilities` | mysqlx_connection.proto |
| 3 | `SESS_AUTHENTICATE_CONTINUE` | `Mysqlx.Session.AuthenticateContinue` | mysqlx_session.proto |
| 4 | `SESS_AUTHENTICATE_OK` | `Mysqlx.Session.AuthenticateOk` | mysqlx_session.proto |
| 11 | `NOTICE` | `Mysqlx.Notice.Frame` | mysqlx_notice.proto |
| 12 | `RESULTSET_COLUMN_META_DATA` | `Mysqlx.Resultset.ColumnMetaData` | mysqlx_resultset.proto |
| 13 | `RESULTSET_ROW` | `Mysqlx.Resultset.Row` | mysqlx_resultset.proto |
| 14 | `RESULTSET_FETCH_DONE` | `Mysqlx.Resultset.FetchDone` | mysqlx_resultset.proto |
| 15 | `RESULTSET_FETCH_SUSPENDED` | `Mysqlx.Resultset.FetchSuspended` | mysqlx_resultset.proto |
| 16 | `RESULTSET_FETCH_DONE_MORE_RESULTSETS` | `Mysqlx.Resultset.FetchDoneMoreResultsets` | mysqlx_resultset.proto |
| 17 | `SQL_STMT_EXECUTE_OK` | `Mysqlx.Sql.StmtExecuteOk` | mysqlx_sql.proto |
| 18 | `RESULTSET_FETCH_DONE_MORE_OUT_PARAMS` | `Mysqlx.Resultset.FetchDoneMoreOutParams` | mysqlx_resultset.proto |
| 19 | `COMPRESSION` | `Mysqlx.Connection.Compression` | mysqlx_connection.proto |

- `NOTICE` の値 11 は恒久的に固定とされている (mysqlx.proto の enum 内コメント「NOTICE has to stay at 11 forever」)
