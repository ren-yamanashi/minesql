# コマンドディスパッチャの詳細仕様 (X Plugin)

- [command_dispatcher.md](../command_dispatcher.md) の論理モデルに対する詳細仕様
- MySQL 8.4 (commit `aa461240`) の `plugin/x/src` を参照
- リクエストを受け取ってから終端を送るまでの順に書く

## 種別ごとの振り分け

- セッションは `Session.Close` / `Connection.Close` / `Session.Reset` と kill 済みの確認を自分で処理し、それ以外のリクエストをディスパッチャに渡す
- ディスパッチャは Expect ブロックの事前判定を通したリクエストを種別で振り分け、処理がエラーを返したら `Error` を送り、Expect ブロックの事後判定を行う
  - 参照:
    - [xpl_dispatcher.cc の execute](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/plugin/x/src/xpl_dispatcher.cc#L33-L44)
    - [dispatch](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/plugin/x/src/xpl_dispatcher.cc#L46-L119)

| 種別 (値) | 処理 | 終端 | 更新される状態変数 |
| --- | --- | --- | --- |
| `SQL_STMT_EXECUTE` (12) | namespace に応じて SQL の実行、または管理コマンドの実行 | `StmtExecuteOk` / `Error` | `Mysqlx_stmt_execute_sql` / `Mysqlx_stmt_execute_mysqlx` |
| `EXPECT_OPEN` (24) / `EXPECT_CLOSE` (25) | Expect ブロックの開閉 | `Ok` / `Error` | `Mysqlx_expect_open` / `Mysqlx_expect_close` |
| `CRUD_*` (17〜20、30〜32) | ドキュメントモデルの操作 (実装対象外) | | `Mysqlx_crud_*` |
| `PREPARE_*` (40〜42) / `CURSOR_*` (43〜45) | プリペアドステートメントとカーソル (実装対象外) | | `Mysqlx_prep_*` / `Mysqlx_cursor_*` |
| それ以外 | 通常の `Error` (code 1047 `ER_UNKNOWN_COM_ERROR` "Unexpected message received") を返し、セッションは継続する | `Error` | `Mysqlx_errors_unknown_message_type` |

- 種別の値は [mysqlx.proto の `ClientMessages`](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/plugin/x/protocol/protobuf/mysqlx.proto#L165-L199) を参照
- 処理の結果の深刻度に応じて、セッションは `ERROR` なら次のリクエストへ進み、`FATAL` ならセッションを閉じる
  - 参照:
    - [session.cc の handle_ready_message](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/plugin/x/src/session.cc#L335-L357)
- `Session.Reset` (`keep_open = true`) ではディスパッチャの状態のうちプリペアドステートメントだけが破棄され、Expect スタックは維持される
  - 参照:
    - [xpl_dispatcher.cc の reset](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/plugin/x/src/xpl_dispatcher.cc#L136-L138)
    - [session.cc の on_reset](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/plugin/x/src/session.cc#L376-L384)

## StmtExecute の処理

- メッセージの定義は [mysqlx_sql.proto](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/plugin/x/protocol/protobuf/mysqlx_sql.proto#L38-L77)、フィールドの意味は [message_spec.md](../../protocol/message_spec.md#sql-実行-sqlstmtexecute) を参照

### namespace の判定

- `namespace` が省略されているか `"sql"` なら SQL ステートメントとして実行する
- `"mysqlx"` なら管理コマンドとして実行する
- それ以外は `Error` (`ER_X_INVALID_NAMESPACE` "Unknown namespace %s") を返す
- 参照:
  - [stmt_command_handler.cc の execute](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/plugin/x/src/stmt_command_handler.cc#L41-L53)

### ステートメントの組み立て (args の埋め込み)

- `stmt` の文字列中のプレースホルダ `?` を、`args` の値で先頭から順に置き換えた 1 つの SQL ステートメント字列を作り、SQL 層にはその文字列だけを渡す (SQL 層側にパラメータは渡さない)
  - 引用符 (`'`、`"`)、識別子の引用 (`` ` ``)、コメント (`/* */`、`-- ` (後ろに空白)、`#`) の内側にある `?` はプレースホルダとして扱わない
- 値の埋め込み方
  - 文字列: MySQL の規則でエスケープし、単引用符で囲む
  - NULL: `NULL` というリテラルをそのまま埋め込む
  - 数値・真偽値: そのまま埋め込む
- `args` がプレースホルダより多い場合は `Error` (`ER_X_CMD_NUM_ARGUMENTS` "Too many arguments") を返す
  - 少ない場合は検査せず、残った `?` を含むステートメントがそのまま SQL 層に渡る (SQL 層の構文エラーになる)
- 参照:
  - [sql_statement_builder.cc の build](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/plugin/x/src/sql_statement_builder.cc#L37-L68)
  - [query_formatter.cc のプレースホルダ探索](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/plugin/x/src/query_formatter.cc#L39-L178)
  - [validate_next_tag / put_value_and_escape](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/plugin/x/src/query_formatter.cc#L217-L240)

### 実行とリザルトセットのストリーミング

- 組み立てたステートメントを内部セッションで実行し、結果はコールバックで受け取る (仕組みは後述の「SQL 層での実行」)
  - `compact_metadata` が真なら、列定義は `type` だけを設定して送る
- 列定義: SQL 層から列ごとの定義を受け取って `ColumnMetaData` に変換し、全列が揃った時点でまとめて送ってフラッシュする
  - 変換の内容 (型の対応、フラグ、`catalog` の固定値 `"def"`) は [message_spec.md の ColumnMetaData](../../protocol/message_spec.md#columnmetadata) を参照
  - 送信に失敗した場合は SQL 層に `ER_IO_WRITE_ERROR` "Connection reset by peer" を報告して実行を中断する
- 行: 1 行分の値を受け取るたびに `Row` を送る
  - 行を送るたびに接続の生存と kill を確認する (長いリザルトセットの途中でも kill やシャットダウンを検知できる)
  - 行の途中でエラーが起きた場合は作りかけの行を破棄する
- 完了: リザルトセットを送っていれば `FetchDone` を送り、続けて実行ステータスの Notice と `StmtExecuteOk` を送る
  - リザルトセットを返さないステートメント (`INSERT` など) では `ColumnMetaData` / `Row` / `FetchDone` は送られず、Notice と `StmtExecuteOk` だけが返る
  - 複数のリザルトセットを返すステートメントでは、リザルトセットの間に `FetchDoneMoreResultsets` を、OUT パラメータのリザルトセットの前に `FetchDoneMoreOutParams` を送る
- 実行ステータスの Notice の内訳と順序
  - `ROWS_AFFECTED` はステートメントの種類によらず常に送る
  - `GENERATED_INSERT_ID` は値が 0 より大きいときだけ送る
  - `PRODUCED_MESSAGE` はサーバーからのメッセージがあるときだけ送る
- 参照:
  - [streaming_command_delegate.cc の start_result_metadata](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/plugin/x/src/streaming_command_delegate.cc#L125-L138)
  - [field_metadata](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/plugin/x/src/streaming_command_delegate.cc#L140-L318)
  - [end_result_metadata](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/plugin/x/src/streaming_command_delegate.cc#L320-L339)
  - [start_row / end_row / abort_row](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/plugin/x/src/streaming_command_delegate.cc#L341-L369)
  - [handle_ok](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/plugin/x/src/streaming_command_delegate.cc#L503-L523)
  - [複数リザルトセットと OUT パラメータ](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/plugin/x/src/streaming_command_delegate.cc#L581-L606)
  - [custom_command_delegates.cc の try_send_notices](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/plugin/x/src/custom_command_delegates.cc#L113-L128)

### 警告の送出

- 警告の Notice が有効 (既定) で、実行が警告を出した場合、実行ステータスの Notice と `StmtExecuteOk` の送出を実行の完了後まで遅らせる
  - 理由: 警告の取り出しは内部セッションで `SHOW WARNINGS` を実行して行うため、ステートメントの実行中には行えない
  - 順序は `FetchDone` → 警告の Notice (`Warning`、警告の数だけ) → 実行ステータスの Notice → `StmtExecuteOk` になる
- 実行がエラーで終わった場合も、警告の Notice が有効なら `Error` を送る前に警告を Notice で送る (エラーそのものと重複する 1 件は除く)
- 参照:
  - [streaming_command_delegate.cc の defer_on_warning](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/plugin/x/src/streaming_command_delegate.cc#L556-L579)
  - [on_destruction](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/plugin/x/src/streaming_command_delegate.cc#L545-L554)
  - [notices.cc の send_warnings](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/plugin/x/src/notices.cc#L110-L118)
  - [stmt_command_handler.cc の sql_stmt_execute](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/plugin/x/src/stmt_command_handler.cc#L57-L85)

### エラー時の挙動

- SQL 層がエラーを返した場合、複数リザルトセットの途中なら `FetchDoneMoreResultsets` を送ってから、`Error` を送る
- 実行中に内部セッションが KILL されていた場合は、エラーを `FATAL` に格上げしてセッションを閉じる
- 参照:
  - [streaming_command_delegate.cc の handle_error](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/plugin/x/src/streaming_command_delegate.cc#L525-L534)
  - [sql_data_context.cc の execute_server_command](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/plugin/x/src/sql_data_context.cc#L632-L658)

## 管理コマンド (namespace `mysqlx`)

- `stmt` にコマンド名 (大文字小文字を区別しない)、`args` に名前付き引数を持つ `Object` を 1 つ渡す
- 共通の前処理
  - パスワードが期限切れの利用者は `Error` (`ER_MUST_CHANGE_PASSWORD`)
  - コマンド名が空なら `Error` (`ER_INTERNAL_ERROR`)、未知なら `Error` (`ER_X_INVALID_ADMIN_COMMAND` "Invalid %s command %s")
  - 引数の過不足や型違いは `Error` (`ER_X_CMD_INVALID_ARGUMENT`)
- 参照:
  - [admin_cmd_handler.cc のコマンド表](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/plugin/x/src/admin_cmd_handler.cc#L96-L115)
  - [Command_handler::execute](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/plugin/x/src/admin_cmd_handler.cc#L117-L137)
  - [Admin_command_handler::execute](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/plugin/x/src/admin_cmd_handler.cc#L142-L155)

| コマンド | 引数 | レスポンス |
| --- | --- | --- |
| `ping` | なし | `StmtExecuteOk` のみ |
| `list_clients` | なし | リザルトセット (`client_id` UINT、`user` BYTES、`host` BYTES、`sql_session` UINT) + `FetchDone` + `StmtExecuteOk` |
| `kill_client` | `id` (uint、必須) | `StmtExecuteOk` のみ (対象の接続を閉じる挙動は [connection/ の kill](../../connection/reference/connection_handler_spec.md#タイムアウトと強制切断) を参照) |
| `enable_notices` | `notice` (文字列のリスト、必須) | `StmtExecuteOk` のみ |
| `disable_notices` | `notice` (文字列のリスト、必須) | `StmtExecuteOk` のみ |
| `list_notices` | なし | リザルトセット (`notice` BYTES、`enabled` SINT) + `FetchDone` + `StmtExecuteOk` |

- `list_clients` の可視範囲: リクエスト元に SUPER 権限があれば全接続、なければ自分と同じ利用者で認証済みの接続だけを列挙する
  - `user` と `sql_session` (内部セッションの ID) は認証済みの接続にだけ入り、未認証の接続では NULL
- Notice の設定
  - 切り替えられる Notice は `warnings` (既定で有効) と、グループレプリケーション関連の 4 つ (既定で無効)
    - `group_replication/membership/quorum_loss`、`group_replication/membership/view`、`group_replication/status/role_change`、`group_replication/status/state_change`
  - `account_expired`、`generated_insert_id`、`rows_affected`、`produced_message` は常に有効で、`enable_notices` では黙って受理され、`disable_notices` では `Error` (`ER_X_CANNOT_DISABLE_NOTICE` "Cannot disable notice %s")
  - 未知の名前は `Error` (`ER_X_BAD_NOTICE` "Invalid notice name %s")
  - `list_notices` は切り替え可能な Notice を現在の状態とともに、常に有効な Notice を `enabled = 1` で列挙する
- 参照:
  - [ping](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/plugin/x/src/admin_cmd_handler.cc#L160-L168)
  - [list_clients](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/plugin/x/src/admin_cmd_handler.cc#L220-L281)
  - [kill_client](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/plugin/x/src/admin_cmd_handler.cc#L287-L304)
  - [常に有効な Notice の名前](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/plugin/x/src/admin_cmd_handler.cc#L384-L393)
  - [enable_notices](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/plugin/x/src/admin_cmd_handler.cc#L410-L439)
  - [disable_notices](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/plugin/x/src/admin_cmd_handler.cc#L445-L477)
  - [list_notices](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/plugin/x/src/admin_cmd_handler.cc#L482-L522)
  - [mq/notice_configuration.h の既定値と名前](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/plugin/x/src/mq/notice_configuration.h#L44-L106)
  - [session.cc の can_see_user](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/plugin/x/src/session.cc#L394-L402)

## Expect ブロック

- メッセージの定義は [mysqlx_expect.proto](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/plugin/x/protocol/protobuf/mysqlx_expect.proto#L51-L97)
- `Expect.Open`
  - `op` が `EXPECT_CTX_COPY_PREV` (既定) なら外側のブロックの条件を引き継ぎ、`EXPECT_CTX_EMPTY` なら空の条件から始める
  - `cond` の各要素は条件の `condition_key`、`condition_value`、`op` (`EXPECT_OP_SET` / `EXPECT_OP_UNSET`) で、順に適用する
  - 条件の種類: `EXPECT_NO_ERROR` (1)、`EXPECT_FIELD_EXIST` (2、指定した protobuf のフィールドをサーバーが扱えるかの機能検出)、`EXPECT_DOCID_GENERATED` (3、ドキュメントモデル向け)
    - 実装するのは `no_error` のみ
    - 公式ドキュメントの Expectations のページには `schema_version` や `gtid_*` といった条件が載っているが、`.proto` と実装にあるのは上の 3 つ (ドキュメントと実装の差)
  - `no_error` の値は `"1"` または空で有効、`"0"` で無効、それ以外は `Error` (`ER_X_EXPECT_BAD_CONDITION_VALUE`)
  - 未知の条件は `Error` (`ER_X_EXPECT_BAD_CONDITION` "Unknown condition key: %u")
  - 失敗中のブロックの中で `Open` が来た場合は、失敗状態のブロックを積んだうえで `Error` を返す (対応する `Close` で正しく取り出せるようにするため)
  - 成功なら `Ok`
- 事前判定: 一番内側のブロックが失敗状態なら、`Expect.Open` / `Expect.Close` 以外のリクエストはすべてそのブロックの失敗理由の `Error` で拒否する
- 事後判定: リクエストがエラーになったとき、一番内側のブロックが `no_error` を持ちまだ失敗していなければ、`Error` (`ER_X_EXPECT_NO_ERROR_FAILED` "Expectation failed: no_error") を理由に失敗状態にする
- `Expect.Close`
  - ブロックが開いていなければ `Error` (`ER_X_EXPECT_NOT_OPEN` "Expect block currently not open")
  - 一番内側のブロックを取り出し、失敗状態だったならその失敗理由の `Error`、そうでなければ `Ok` を返す
- 入れ子の意味論 (実装内の doc コメントに整理されている 6 通り)
  - `no_error` の中の `no_error`: 内側の失敗は外側にも伝わる
  - `no_error` の中の条件なし: 条件なしのブロックが失敗を吸収し、外側は失敗しない
  - 条件なしの中の `no_error`: 内側は失敗するが、外側は影響を受けない
- 参照:
  - [expect_stack.cc の open](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/plugin/x/src/expect/expect_stack.cc#L44-L92)
  - [close](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/plugin/x/src/expect/expect_stack.cc#L94-L109)
  - [pre_client_stmt / post_client_stmt](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/plugin/x/src/expect/expect_stack.cc#L111-L146)
  - [expect.cc の入れ子の意味論 (doc コメント)](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/plugin/x/src/expect/expect.cc#L40-L125)
  - [Expectation::set / unset](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/plugin/x/src/expect/expect.cc#L164-L212)
  - [xpl_dispatcher.cc の on_expect_open / on_expect_close](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/plugin/x/src/xpl_dispatcher.cc#L122-L134)

## SQL 層での実行

- ディスパッチャは SQL 層に「内部セッション上でステートメントを実行し、結果をコールバックで返す」ことだけを依頼する
  - minesql ではこの依頼先が SQL 層 (parser/ 以降) に直結する
- MySQL では、この依頼はプラグイン向けの command service を通り、`COM_*` 層 (classic protocol と共通のコマンドの入口) に入る
  - X Plugin が使う `COM_*` は `COM_QUERY` (SQL ステートメント)、`COM_RESET_CONNECTION` (`Session.Reset` の `keep_open`)、`COM_INIT_DB` (既定スキーマの切り替え)、`COM_STMT_PREPARE` / `EXECUTE` / `FETCH` / `CLOSE` (プロトコルのプリペアドステートメント、実装対象外) だけ
  - command service は内部セッションをスレッドに結び付け、コールバック集を proxy の `Protocol` として差し込んでから、classic protocol と同じ `dispatch_command` を呼ぶ
- コールバックは 3 群に分かれる
  - メタデータ: リザルトセットの開始、列ごとの定義、メタデータの終了
  - データ: 行の開始と終了、値の型ごとの受け渡し (NULL、整数、小数、浮動小数点、日付時刻、文字列)
  - ステータス: 完了 (Affected Rows、Last Insert ID、警告数、サーバーの状態フラグ、メッセージ)、エラー、シャットダウン、接続の生存確認
- 接続の生存確認のコールバックでは、ソケットの状態に加えて読み取り前のチェック (kill やシャットダウンの検知) も実行する
- 参照:
  - [sql_data_context.cc の execute / execute_sql](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/plugin/x/src/sql_data_context.cc#L541-L566)
  - [execute_server_command](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/plugin/x/src/sql_data_context.cc#L632-L658)
  - [include/mysql/service_command.h のコールバック集](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/include/mysql/service_command.h#L328-L387)
  - [sql/srv_session.cc の execute_command](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/sql/srv_session.cc#L1120-L1189)
  - [sql/protocol_callback.h](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/sql/protocol_callback.h#L27-L60)
  - [sql/sql_parse.cc の dispatch_command](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/sql/sql_parse.cc#L1725-L1741)
  - [streaming_command_delegate.cc の connection_alive](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/plugin/x/src/streaming_command_delegate.cc#L608-L620)

## エラーと深刻度

| 事象 | 深刻度 | エラー |
| --- | --- | --- |
| 未知の種別 | `ERROR` | 1047 `ER_UNKNOWN_COM_ERROR` "Unexpected message received" |
| 未知の namespace | `ERROR` | `ER_X_INVALID_NAMESPACE` "Unknown namespace %s" |
| `args` がプレースホルダより多い | `ERROR` | `ER_X_CMD_NUM_ARGUMENTS` "Too many arguments" |
| SQL 層の実行エラー (構文エラー、権限エラーなど) | `ERROR` | SQL 層のエラーをそのまま返す |
| 管理コマンドの名前・引数の誤り | `ERROR` | `ER_X_INVALID_ADMIN_COMMAND` / `ER_X_CMD_INVALID_ARGUMENT` / `ER_X_BAD_NOTICE` / `ER_X_CANNOT_DISABLE_NOTICE` |
| Expect ブロックの失敗・誤用 | `ERROR` | `ER_X_EXPECT_NO_ERROR_FAILED` / `ER_X_EXPECT_NOT_OPEN` / `ER_X_EXPECT_BAD_CONDITION` / `ER_X_EXPECT_BAD_CONDITION_VALUE` |
| 実行中に内部セッションが KILL された | `FATAL` | SQL 層のエラーを `FATAL` に格上げ |
| SQL 層との連携の失敗 (実行の依頼自体が失敗、利用者の切り替えの失敗) | `ERROR` または `FATAL` | `ER_X_SERVICE_ERROR` |

- `ERROR` ではシーケンスだけが中断され、セッションは次のリクエストを受け付ける
- `FATAL` ではセッションが閉じられ、接続の終了処理に入る ([connection/ の接続のライフサイクル](../../connection/reference/connection_handler_spec.md#接続のライフサイクル))
- 参照:
  - [sql_data_context.cc の execute_server_command (KILL 時の格上げ)](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/plugin/x/src/sql_data_context.cc#L632-L658)
  - [switch_to_user (FATAL の `ER_X_SERVICE_ERROR`)](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/plugin/x/src/sql_data_context.cc#L445-L455)

## 状態変数

ディスパッチャの処理に関わるステータス変数 (定義は [status_variables.cc](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/plugin/x/src/variables/status_variables.cc#L262-L369))

- ステートメントの実行: `Mysqlx_stmt_execute_sql` / `Mysqlx_stmt_execute_mysqlx`
- 管理コマンド: `Mysqlx_stmt_ping` / `Mysqlx_stmt_list_clients` / `Mysqlx_stmt_kill_client` / `Mysqlx_stmt_enable_notices` / `Mysqlx_stmt_disable_notices` / `Mysqlx_stmt_list_notices`
- Expect ブロック: `Mysqlx_expect_open` / `Mysqlx_expect_close`
- 送信: `Mysqlx_rows_sent` / `Mysqlx_messages_sent` / `Mysqlx_notice_warning_sent` / `Mysqlx_notice_other_sent` / `Mysqlx_notice_global_sent`
- エラー: `Mysqlx_errors_sent` / `Mysqlx_errors_unknown_message_type`
- 実装対象外の種別に対応するもの: `Mysqlx_crud_*` / `Mysqlx_prep_*` / `Mysqlx_cursor_*` / コレクション系と `list_objects` の `Mysqlx_stmt_*`

## 論理モデルの主張とソースの対応

論理モデルの文書から移した、各主張に対応する MySQL のソースへの参照

### [command_dispatcher.md](../command_dispatcher.md) より

- ディスパッチャ (Dispatcher): セッションごとに 1 つ
  - [xpl_dispatcher.h](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/plugin/x/src/xpl_dispatcher.h#L40-L60)
- 結果の受け口 (デリゲート): 実行 1 回につき 1 つ作られ、SQL 層からのコールバック (列定義、行、完了、エラー) を受けてプロトコルのメッセージに変換して送る
  - [ngs/command_delegate.h](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/plugin/x/src/ngs/command_delegate.h#L41-L80)
- Expect スタック: セッションごとに 1 つ持つ、開いている Expect ブロックの入れ子
  - [expect/expect_stack.cc](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/plugin/x/src/expect/expect_stack.cc#L34-L41)
