# コネクションハンドラーの詳細仕様 (X Plugin)

- [connection_handler.md](./connection_handler.md) の論理モデルに対する詳細仕様
- MySQL 8.4 (commit `aa461240`) の `plugin/x/src` を参照
- 起動から終了までの時系列に沿って書く

## 起動と接続の待ち受け

- listener の準備
  - TCP: `mysqlx_bind_address` (カンマ区切りで複数指定可) の各アドレスに対して `mysqlx_port` で listen する
    - サーバー全体の `skip_networking` が ON なら TCP の listener は作らない
    - 各 listener はソケット作成 → `bind` → `listen` (backlog 指定) の順で用意する
      - `bind` がアドレス使用中 (`EADDRINUSE`) で失敗した場合だけ、`mysqlx_port_open_timeout` 秒まで間隔を広げながら再試行する
      - 参照:
        - [xpl_listener_tcp.cc の create_and_bind_socket](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/plugin/x/src/io/xpl_listener_tcp.cc#L101-L190)
        - [create_socket の再試行ループ](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/plugin/x/src/io/xpl_listener_tcp.cc#L348-L369)
  - Unix ソケット: `mysqlx_socket` で listen する
    - classic protocol 用の `socket` と同じパスなら警告を出して作らない
  - backlog は `50 + mysqlx_max_connections / 5` (上限 900)
  - 準備できた listener が 1 つもなければ起動失敗
  - 参照:
    - [socket_acceptors_task.cc の prepare_listeners](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/plugin/x/src/ngs/socket_acceptors_task.cc#L145-L200)
    - [server_builder.cc の backlog](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/plugin/x/src/server/builder/server_builder.cc#L57-L66)
- accept の開始は、srv_session サービスが利用可能になるまで遅延する (プラグインのロード時点ではサーバーがまだ SQL を実行できる状態にないため)
  - 参照:
    - [server.cc の delayed_start_tasks](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/plugin/x/src/server/server.cc#L109-L148)
- ワーカースケジューラは起動時に `mysqlx_min_worker_threads` 本のスレッドを作る
  - 参照:
    - [scheduler.cc の launch](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/plugin/x/src/ngs/scheduler.cc#L62-L76)

## 接続の受付

- acceptor スレッドが listen ソケットの読み取り可能イベントを受けて accept する (`EINTR` / `EAGAIN` は最大 10 回リトライ)
  - 受け付けたソケットには `TCP_NODELAY` と keepalive を設定する
  - 参照:
    - [socket_events.cc の accept](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/plugin/x/src/ngs/socket_events.cc#L55-L89)
    - [callback_accept_socket](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/plugin/x/src/ngs/socket_events.cc#L196-L212)
- 受け入れ判定
  - サーバーが終了中なら拒否する
  - 現在の接続数が `mysqlx_max_connections` 以上なら拒否する (警告ログ、`Mysqlx_connections_rejected` を加算)
  - 拒否した接続にはプロトコル上の応答を返さず、ソケットを閉じる
  - 受理した接続は接続一覧に登録し (`Mysqlx_connections_accepted` を加算)、accept 時刻を記録する
  - 参照:
    - [server.cc の will_accept_client](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/plugin/x/src/server/server.cc#L325-L348)
- 受理した接続の処理をタスクとしてワーカースケジューラに投入し、接続タイムアウトの監視タイマーが止まっていれば起動する
  - 投入に失敗した場合は一覧から除去する
  - 参照:
    - [server.cc の on_accept](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/plugin/x/src/server/server.cc#L350-L401)
- サーバー全体の `max_connections` との関係: X の接続数は `mysqlx_max_connections` で独立に数えるが、内部セッションを開く際にサーバー全体の接続数制限にも掛かる (超過時は `ER_CON_COUNT_ERROR` がクライアントに返る)
  - 参照:
    - [system_variables.cc の max_connections の説明](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/plugin/x/src/variables/system_variables.cc#L122-L130)
    - [sql_data_context.cc](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/plugin/x/src/sql_data_context.cc#L111-L112)

## ワーカースレッドプール

- 動的プール
  - 起動時に最小本数 (`mysqlx_min_worker_threads`、既定 2) を作る
  - タスク投入時、「実行中 + 待機中のタスク数 >= スレッド数」なら新しいスレッドを作る
    - そのためタスクがキューで待たされることは基本的になく、同時接続数に応じてスレッドが増える
  - タスクが無くアイドルになったスレッドは、`mysqlx_idle_worker_thread_timeout` (既定 60 秒) の経過後、スレッド数が最小本数を上回っていれば終了する
  - 終了したスレッドは acceptor スレッドの 1 秒周期のタイマーで join される
  - 各ワーカーはスレッド開始時に srv_session サービスへ登録され、内部セッションを扱えるスレッドになる
  - 参照:
    - [scheduler.cc の post](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/plugin/x/src/ngs/scheduler.cc#L132-L156)
    - [wait_if_idle_then_delete_worker](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/plugin/x/src/ngs/scheduler.cc#L178-L214)
    - [worker](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/plugin/x/src/ngs/scheduler.cc#L216-L264)
    - [server.cc の join タイマー](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/plugin/x/src/server/server.cc#L172-L175)
    - [session_scheduler.cc の thread_init](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/plugin/x/src/server/session_scheduler.cc#L47-L62)
- 状態変数: `Mysqlx_worker_threads` (存在するワーカースレッド数)、`Mysqlx_worker_threads_active` (タスク実行中のスレッド数)
  - 参照:
    - [scheduler_monitor.h](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/plugin/x/src/server/scheduler_monitor.h#L34-L51)

## 接続のライフサイクル

状態の定義と遷移図は [connection_handler.md の状態遷移](./connection_handler.md#接続とセッションの状態遷移) を参照。ここでは各状態での処理を書く

- ワーカースレッド上の処理の流れ
  1. 接続元アドレスの取得
  2. 受付処理: 状態を `accepted` にし、エンコーダを作り、セッションを事前に生成する
     - ここで内部セッション (THD) を開く (失敗時は Fatal エラーを返して切断)
     - `mysqlx_enable_hello_notice` が ON なら `ServerHello` の Notice を送る
  3. ループ: 状態が `closing` になるまで、1 メッセージを読んで dispatch する処理を繰り返す
     - フレームのデコードエラーは Fatal エラーを返して切断する
  4. 後始末: 切断理由の Notice を送出し、状態を `closed` にして接続一覧から除去する (`Mysqlx_connections_closed` を加算)
  - 参照:
    - [client.cc の on_accept](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/plugin/x/src/client.cc#L450-L490)
    - [run](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/plugin/x/src/client.cc#L606-L641)
    - [create_session](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/plugin/x/src/client.cc#L686-L726)
    - [vio_wrapper.cc の read / write / shutdown](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/plugin/x/src/ngs/vio_wrapper.cc#L38-L108)
- 認証前 (`accepted`) に受け付けるメッセージ (Client 自身が処理する)
  - `CapabilitiesGet` / `CapabilitiesSet`: capability の取得・設定
  - `Connection.Close`: `Ok` ("bye!") を返して閉じる
  - `AuthenticateStart`: `authenticating_first` に遷移し、以後のメッセージはセッションへ転送する
  - `Session.Reset`: 無視する
  - それ以外: Fatal エラー (`ER_X_BAD_MESSAGE` "Invalid message") を返して切断する
  - 参照:
    - [client.cc の handle_message](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/plugin/x/src/client.cc#L264-L337)
- セッションの状態は `authenticating → ready → closing` と進む
  - `authenticating`: `AuthenticateStart` / `AuthenticateContinue` のみ受け付ける
    - 未知のメカニズムは Fatal エラーで切断する
    - 認証失敗は最大 3 回まで許容し (それまでは通常エラーで、別のメカニズムでの再試行が可能)、3 回目の失敗は Fatal エラーで切断する
  - 認証成功時: クライアント ID の Notice を送り、Client を `running` にしてから `AuthenticateOk` を返す (`Mysqlx_sessions_accepted` を加算)
  - `ready`: `Session.Close` / `Connection.Close` / `Session.Reset` はセッション自身が処理し、それ以外はディスパッチャに渡す
    - 致命的エラーはセッションを閉じる
  - 参照:
    - [session.cc の handle_auth_message](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/plugin/x/src/session.cc#L141-L202)
    - [on_auth_failure_impl](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/plugin/x/src/session.cc#L222-L250)
    - [session.h の k_max_auth_attempts](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/plugin/x/src/session.h#L125)
    - [on_auth_success](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/plugin/x/src/session.cc#L204-L220)
    - [handle_ready_message](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/plugin/x/src/session.cc#L297-L357)
- セッションの再生成
  - `Session.Reset` (`keep_open = true`): 接続もセッションも維持したまま、内部セッションの状態をリセットして `Ok` を返す
  - `Session.Reset` (`keep_open` なし / false) と `Session.Close`: 現在のセッションを閉じ、新しいセッションを事前生成して `Ok` を返す
    - Client は `accepted_with_session` (再認証待ち) になる
    - この状態で受け付けるのは `CapabilitiesSet` (`session_connect_attrs` のみ) と認証メッセージだけで、それ以外は Fatal エラーで切断する
    - 再認証が成功しても Client の状態は `accepted_with_session` のままで、以後の判定はセッションの状態 (`authenticating` / `ready`) による
  - 参照:
    - [session.cc の on_reset](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/plugin/x/src/session.cc#L376-L384)
    - [client.cc の on_session_reset](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/plugin/x/src/client.cc#L524-L533)
    - [handle_session_connect_attr_set](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/plugin/x/src/client.cc#L244-L262)
    - [on_session_auth_success](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/plugin/x/src/client.cc#L492-L514)

## タイムアウトと強制切断

- 接続タイムアウト (`mysqlx_connect_timeout`、既定 30 秒): accept から初回の認証完了までの上限
  - acceptor スレッドのタイマーが未認証 (`invalid` / `accepted` / `authenticating_first`) のクライアントを走査し、超過したものを切断する (`Mysqlx_connection_errors` を加算)
  - タイマーは最も古い未認証クライアントの期限に合わせて再スケジュールされる
  - `Session.Close` 後の再認証待ち (`accepted_with_session`) は対象外
  - 参照:
    - [server.cc の timeout_for_clients_validation](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/plugin/x/src/server/server.cc#L283-L323)
    - [server_client_timeout.cc](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/plugin/x/src/ngs/server_client_timeout.cc#L36-L58)
    - [client.cc の on_auth_timeout](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/plugin/x/src/client.cc#L193-L197)
- アイドルタイムアウト (`mysqlx_wait_timeout`、既定 28800 秒): 次のメッセージのヘッダ (先頭 4 バイト) を待つ時間の上限
  - capability `client.interactive` が設定された接続では、代わりに `mysqlx_interactive_timeout` (既定 28800 秒) を使う
  - 参照:
    - [protocol_decoder.cc の read_header](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/plugin/x/src/ngs/protocol_decoder.cc#L58-L126)
    - [client.cc の set_is_interactive](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/plugin/x/src/client.cc#L868-L889)
- 読み取りタイムアウト (`mysqlx_read_timeout`、既定 30 秒): ヘッダを受信してからメッセージの残りを読み終えるまでの上限
  - 超過すると "IO Read error" の Notice を送って切断する (`Mysqlx_aborted_clients` を加算)
  - 参照:
    - [client.cc の on_read_timeout](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/plugin/x/src/client.cc#L366-L369)
    - [update_counters](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/plugin/x/src/client.cc#L389-L407)
- 書き込みタイムアウト (`mysqlx_write_timeout`、既定 60 秒): 応答の書き込みがブロックできる時間の上限
  - 超過はネットワークエラーとして切断する
  - 参照:
    - [client.cc の on_network_error](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/plugin/x/src/client.cc#L375-L387)
- 最大メッセージ長 (`mysqlx_max_allowed_packet`、既定 64 MB): フレームの `length` が超過していれば応答を返さずに切断する
  - 参照:
    - [protocol_decoder.cc の read_and_decode_impl](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/plugin/x/src/ngs/protocol_decoder.cc#L138-L188)
- kill
  - 管理コマンド `kill_client` で他の接続を閉じられる
    - 対象が自分自身なら即座に閉じ、他の接続なら対応する内部セッションを KILL してから Client を閉じる
  - アイドル中 (メッセージ待ち) の接続は、読み取り前のチェックで kill を検知して閉じる
  - 参照:
    - [server.cc の kill_client](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/plugin/x/src/server/server.cc#L442-L502)
    - [client.cc の kill](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/plugin/x/src/client.cc#L555-L570)
    - [Client_idle_reporting](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/plugin/x/src/client.cc#L87-L127)
    - [admin_cmd_handler.cc のコマンド表](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/plugin/x/src/admin_cmd_handler.cc#L99-L100)
- シャットダウン: listener を止め、全接続に終了を通知し、全接続が閉じるまで最大 5 秒 (0.25 秒 × 20 回) 待つ
  - 残っていればログを出して先に進み、スケジューラを停止する
  - 参照:
    - [server.cc の stop](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/plugin/x/src/server/server.cc#L197-L227)
    - [wait_for_clients_closure](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/plugin/x/src/server/server.cc#L266-L281)
    - [client.cc の on_server_shutdown](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/plugin/x/src/client.cc#L535-L553)
    - [socket_acceptors_task.cc の post_loop (全 listener の close)](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/plugin/x/src/ngs/socket_acceptors_task.cc#L318-L327)

## 設定変数とステータス変数

接続処理に関わる設定変数 (既定値は [system_variables_defaults.h](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/plugin/x/src/variables/system_variables_defaults.h#L41-L64)、定義は [system_variables.cc](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/plugin/x/src/variables/system_variables.cc#L115-L272))

| 変数 | 既定値 | 意味 |
| --- | --- | --- |
| `mysqlx_port` | 33060 | TCP の listen ポート |
| `mysqlx_bind_address` | `*` | TCP の bind アドレス (カンマ区切りで複数可) |
| `mysqlx_socket` | ビルド時定義 | Unix ソケットのパス |
| `mysqlx_port_open_timeout` | 0 | bind 失敗時にリトライを続ける秒数 |
| `mysqlx_max_connections` | 100 | 同時接続数の上限 |
| `mysqlx_min_worker_threads` | 2 | ワーカースレッドの最小本数 |
| `mysqlx_idle_worker_thread_timeout` | 60 | アイドルなワーカースレッドを終了するまでの秒数 |
| `mysqlx_max_allowed_packet` | 64 MB | 受け付けるフレームの最大長 |
| `mysqlx_connect_timeout` | 30 | accept から認証完了までの上限秒数 |
| `mysqlx_wait_timeout` | 28800 | 非対話接続のアイドル上限秒数 (セッション変数) |
| `mysqlx_interactive_timeout` | 28800 | 対話接続のアイドル上限秒数 |
| `mysqlx_read_timeout` | 30 | メッセージ読み取りの上限秒数 (セッション変数) |
| `mysqlx_write_timeout` | 60 | 応答書き込みの上限秒数 (セッション変数) |
| `mysqlx_enable_hello_notice` | ON | 接続直後に `ServerHello` を送るか |

接続処理に関わるステータス変数 (定義は [status_variables.cc](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/plugin/x/src/variables/status_variables.cc#L376-L415))

- 接続: `Mysqlx_connections_accepted` / `Mysqlx_connections_closed` / `Mysqlx_connections_rejected` / `Mysqlx_connection_errors` / `Mysqlx_connection_accept_errors` / `Mysqlx_aborted_clients`
- セッション: `Mysqlx_sessions` / `Mysqlx_sessions_accepted` / `Mysqlx_sessions_closed` / `Mysqlx_sessions_rejected` / `Mysqlx_sessions_killed` / `Mysqlx_sessions_fatal_error`
- スレッド: `Mysqlx_worker_threads` / `Mysqlx_worker_threads_active`
