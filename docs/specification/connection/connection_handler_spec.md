# コネクションハンドラーの詳細仕様 (X Plugin)

- [connection_handler.md](./connection_handler.md) の論理モデルに対する詳細仕様
- MySQL 8.4 (commit `aa461240`) の `plugin/x/src` を参照
- 起動から終了までの時系列に沿って書く

## 起動と接続の待ち受け

- listener の準備
  - TCP: `mysqlx_bind_address` (カンマ区切りで複数指定可) の各アドレスに対して `mysqlx_port` で listen する
    - サーバー全体の `skip_networking` が ON なら TCP の listener は作らない
    - 各 listener は `socket` → `bind` → `listen` (backlog 指定) の順で用意する
      - `socket` 〜 `listen` のいずれかがアドレス使用中 (`EADDRINUSE`) で失敗した場合だけ、`mysqlx_port_open_timeout` 秒まで間隔を広げながら再試行する (再試行の判定は直前のソケット操作の errno で行い、操作の種類は見ない)
      - 参照:
        - [xpl_listener_tcp.cc の create_and_bind_socket](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/plugin/x/src/io/xpl_listener_tcp.cc#L95-L201)
        - [create_socket の再試行ループ](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/plugin/x/src/io/xpl_listener_tcp.cc#L348-L369)
  - Unix ソケット: `mysqlx_socket` で listen する
    - classic protocol 用の `socket` と同じパスなら警告を出して作らない
  - backlog は `50 + mysqlx_max_connections / 5` (上限 900)
  - 準備できた listener が 1 つもなければ起動失敗
  - 参照:
    - [socket_acceptors_task.cc の prepare_listeners](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/plugin/x/src/ngs/socket_acceptors_task.cc#L145-L200)
    - [server_builder.cc の backlog](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/plugin/x/src/server/builder/server_builder.cc#L57-L66)
- accept の開始はプラグインのロード時ではなく、サーバー起動完了の通知 (audit の SERVER_STARTUP イベント) を受けてから (プラグインのロード時点ではサーバーがまだ SQL を実行できる状態にないため)
  - 通知を受けて開始するのは SHA256 パスワードキャッシュのモジュールで、それが無効な場合だけ、srv_session サービスが利用可能になるのを待つ遅延起動に切り替わる
  - 加えて accept のたびに srv_session サービスが利用可能かを待つ
  - 参照:
    - [module_cache.cc の起動イベント処理](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/plugin/x/src/module_cache.cc#L46-L53)
    - [module_mysqlx.cc の遅延起動への切り替え](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/plugin/x/src/module_mysqlx.cc#L193-L195)
    - [server.cc の start_tasks / delayed_start_tasks](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/plugin/x/src/server/server.cc#L109-L148)
    - [server.cc の on_accept での待ち](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/plugin/x/src/server/server.cc#L350-L360)
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
  - 拒否した接続にはプロトコル上のレスポンスを返さず、ソケットを閉じる
  - 受理した接続は接続一覧に登録し (`Mysqlx_connections_accepted` を加算)、accept 時刻を記録する
  - 参照:
    - [server.cc の will_accept_client](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/plugin/x/src/server/server.cc#L325-L348)
- 受理した接続の処理をタスクとしてワーカースケジューラに投入する
  - 最初の接続を受理したときに接続タイムアウトの監視タイマーを起動する (以後はタイマー自身が再スケジュールして回り続け、未認証の接続がなくても `mysqlx_connect_timeout` 周期で走査する)
  - 投入に失敗した場合は一覧から除去する
  - 参照:
    - [server.cc の on_accept](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/plugin/x/src/server/server.cc#L350-L401)
    - [restart_client_supervision_timer](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/plugin/x/src/server/server.cc#L296-L300)
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
  1. 接続元アドレスの取得と名前解決
     - Unix ソケットならホスト名は localhost、TCP なら相手のアドレスとポートを取る
     - サーバー全体の `skip_name_resolve` が OFF なら逆引きでホスト名を求め、そのホストが接続失敗の累積でブロックされていれば接続を閉じる
  2. 受付処理: 状態を `accepted` にし、エンコーダを作り、セッションを事前に生成する
     - ここで内部セッション (THD) を開く (失敗時は Fatal エラーを返して切断)
     - `mysqlx_enable_hello_notice` が ON なら `ServerHello` の Notice を送る
  3. ループ: 状態が `closing` になるまで、1 メッセージを読んで dispatch する処理を繰り返す
     - フレームのデコードエラーは Fatal エラーを返して切断する
  4. 後始末: キューに残っている Notice を送出し、状態を `closed` にして接続一覧から除去する (`Mysqlx_connections_closed` を加算)
     - 稼働中に kill またはシャットダウンで閉じる場合は、その旨の警告 Notice をここで積んでから送出する (kill の場合は Fatal エラーを送っていないときだけ)
     - 読み取りタイムアウトの警告は検知した時点で積まれている
     - 内部セッションは、この後ワーカーのタスクが終わって接続オブジェクトが破棄されるときに閉じる
  - 参照:
    - [client.cc の on_accept](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/plugin/x/src/client.cc#L450-L490)
    - [run](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/plugin/x/src/client.cc#L606-L641)
    - [create_session](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/plugin/x/src/client.cc#L686-L726)
    - [vio_wrapper.cc の read / write / shutdown](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/plugin/x/src/ngs/vio_wrapper.cc#L38-L108)
    - [client.cc の on_client_addr](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/plugin/x/src/client.cc#L416-L448)
    - [resolve_hostname](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/plugin/x/src/client.cc#L796-L835)
    - [queue_up_disconnection_notice_if_necessary](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/plugin/x/src/client.cc#L780-L794)
    - [session.cc のデストラクタ](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/plugin/x/src/session.cc#L80-L100)
    - [sql_data_context.cc の deinit](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/plugin/x/src/sql_data_context.cc#L127-L152)
- 認証前 (`accepted`) に受け付けるメッセージ (接続自身が処理する)
  - `CapabilitiesGet` / `CapabilitiesSet`: capability の取得・設定
  - `Connection.Close`: `Ok` ("bye!") を返して閉じる
  - `AuthenticateStart`: `authenticating_first` に遷移し、以後のメッセージはセッションへ転送する
    - サーバーが稼働中でなければ不正なメッセージとして扱う (Fatal エラーで切断)
  - `Session.Reset`: 無視する
  - それ以外: Fatal エラー (`ER_X_BAD_MESSAGE` "Invalid message") を返して切断する
  - 参照:
    - [client.cc の handle_message](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/plugin/x/src/client.cc#L264-L337)
- セッションの状態は `authenticating → ready → closing` と進む
  - `authenticating`: `AuthenticateStart` / `AuthenticateContinue` のみ受け付ける
    - 未知のメカニズムは Fatal エラーで切断する
    - 認証失敗は最大 3 回まで許容し (それまでは通常エラーで、別のメカニズムでの再試行が可能)、3 回目の失敗は Fatal エラーで切断する
  - 認証成功時: クライアント ID の Notice を送り、接続を `running` にしてから `AuthenticateOk` を返す (`Mysqlx_sessions_accepted` と `Mysqlx_sessions` を加算)
  - `ready`: `Session.Close` / `Connection.Close` / `Session.Reset` はセッション自身が処理し、それ以外はディスパッチャに渡す
    - メッセージを受け取るたびに、処理の前に内部セッションが KILL 済みかを確認し、済みなら `ER_QUERY_INTERRUPTED` の Fatal エラーを返してセッションを閉じる
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
    - 接続は `accepted_with_session` (セッション再生成後) になる
    - この状態で受け付けるのは `CapabilitiesSet` (`session_connect_attrs` のみ) と認証メッセージだけ
      - 認証メッセージ以外を送ると Fatal エラーを返して切断する
      - 例外として、`session_connect_attrs` 以外を含む `CapabilitiesSet` には Fatal エラーを返すだけで接続は閉じず、クライアントが FATAL の規約どおり閉じるのを待つ
    - 再認証が成功しても接続の状態は `accepted_with_session` のままで、以後の判定はセッションの状態 (`authenticating` / `ready`) による
      - そのため、接続の状態が `running` であることを前提にした処理 (アイドル中の kill 検知、kill / シャットダウン時の警告 Notice) は働かない
  - 参照:
    - [session.cc の on_reset](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/plugin/x/src/session.cc#L376-L384)
    - [client.cc の on_session_reset](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/plugin/x/src/client.cc#L524-L533)
    - [handle_session_connect_attr_set](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/plugin/x/src/client.cc#L244-L262)
    - [on_session_auth_success](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/plugin/x/src/client.cc#L492-L514)

## タイムアウトと強制切断

- 接続タイムアウト (`mysqlx_connect_timeout`、既定 30 秒): accept から初回の認証完了までの上限
  - acceptor スレッドのタイマーが未認証 (`invalid` / `accepted` / `authenticating_first`) のクライアントを走査し、超過したものを切断する (`Mysqlx_connection_errors` を加算)
  - タイマーは最も古い未認証クライアントの期限に合わせて再スケジュールされる
  - 判定には 100 ms のヒステリシスがあり、期限の 100 ms 手前までの接続もまとめて切断する
  - `Session.Close` 後のセッション再生成後 (`accepted_with_session`) は対象外
  - 参照:
    - [server.cc の timeout_for_clients_validation](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/plugin/x/src/server/server.cc#L283-L323)
    - [server_client_timeout.cc](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/plugin/x/src/ngs/server_client_timeout.cc#L36-L58)
    - [client.cc の on_auth_timeout](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/plugin/x/src/client.cc#L193-L197)
    - [protocol_config.h のヒステリシス](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/plugin/x/src/ngs/protocol/protocol_config.h#L46-L52)
- アイドルタイムアウト (`mysqlx_wait_timeout`、既定 28800 秒): 次のメッセージのヘッダ (先頭 4 バイト) を待つ時間の上限
  - capability `client.interactive` が設定された接続では、代わりに `mysqlx_interactive_timeout` (既定 28800 秒) を使う
  - 待ちは 500 ms (Windows は 1000 ms) 単位で刻まれ、その都度「読み取り前のチェック」(kill やシャットダウンの検知) が入る
  - 超過時は読み取りタイムアウトと同じ経路で処理され、Notice の文言も "read_timeout exceeded" になる (`Mysqlx_aborted_clients` と `Mysqlx_connection_errors` を加算)
  - 参照:
    - [protocol_decoder.cc の read_header](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/plugin/x/src/ngs/protocol_decoder.cc#L58-L126)
    - [protocol_decoder.cc の刻み幅](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/plugin/x/src/ngs/protocol_decoder.cc#L35-L41)
    - [client.cc の read_one_message_and_dispatch](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/plugin/x/src/client.cc#L583-L604)
    - [client.cc の set_is_interactive](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/plugin/x/src/client.cc#L868-L889)
- 読み取りタイムアウト (`mysqlx_read_timeout`、既定 30 秒): ヘッダを受信してからメッセージの残りを読み終えるまでの上限
  - 超過すると "IO Read error: read_timeout exceeded" の警告 Notice を積んで切断する (`Mysqlx_aborted_clients` と `Mysqlx_connection_errors` を加算)
  - 参照:
    - [client.cc の on_read_timeout](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/plugin/x/src/client.cc#L366-L369)
    - [update_counters](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/plugin/x/src/client.cc#L389-L407)
- 書き込みタイムアウト (`mysqlx_write_timeout`、既定 60 秒): レスポンスの書き込みがブロックできる時間の上限
  - 超過はネットワークエラーとして切断する (`Mysqlx_aborted_clients` と `Mysqlx_connection_errors` を加算)
  - 参照:
    - [client.cc の on_network_error](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/plugin/x/src/client.cc#L375-L387)
- 最大メッセージ長 (`mysqlx_max_allowed_packet`、既定 64 MB): フレームの `length` が超過していればレスポンスを返さずに切断する
  - 参照:
    - [protocol_decoder.cc の read_and_decode_impl](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/plugin/x/src/ngs/protocol_decoder.cc#L138-L188)
- kill
  - 管理コマンド `kill_client` で他の接続を閉じられる (`Mysqlx_sessions_killed` を加算)
    - 他の接続なら、リクエスト元のセッションで対象の内部セッションに `KILL <id>` ステートメントを実行してから接続を閉じる
    - 自分自身なら、プラグイン内部のユーザーで別の内部セッションを開き、そこから自分の内部セッションに `KILL <id>` ステートメントを実行してから閉じる (機構は同じ)
    - 実体が `KILL` ステートメントなので、classic protocol の接続から `KILL <id>` を実行した場合も同じ経路で閉じる
    - `KILL QUERY` は実行中のステートメントを止めるだけで、接続は閉じない
  - 検知経路は 2 つ
    - アイドル中 (メッセージ待ち) の接続は、読み取り前のチェックで kill を検知して閉じる (接続の状態が `running` のときだけ)
    - メッセージを受け取った接続は、処理の前に KILL 済みかを確認し、`ER_QUERY_INTERRUPTED` の Fatal エラーを返して閉じる (セッション再生成後の接続はこちらだけ)
  - 参照:
    - [sql_data_context.cc の execute_kill_sql_session](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/plugin/x/src/sql_data_context.cc#L504-L510)
    - [is_killed](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/plugin/x/src/sql_data_context.cc#L520-L527)
    - [Sql_data_context::kill (自分自身の kill)](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/plugin/x/src/sql_data_context.cc#L170-L214)
    - [session.cc の handle_ready_message の冒頭](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/plugin/x/src/session.cc#L297-L307)
    - [server.cc の kill_client](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/plugin/x/src/server/server.cc#L442-L502)
    - [client.cc の kill](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/plugin/x/src/client.cc#L555-L570)
    - [Client_idle_reporting](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/plugin/x/src/client.cc#L87-L127)
    - [admin_cmd_handler.cc のコマンド表](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/plugin/x/src/admin_cmd_handler.cc#L99-L100)
- シャットダウン: listener を止め、全接続に終了を通知し、全接続が閉じるまで最大約 4.75 秒 (0.25 秒の待ちを最大 19 回) 待つ
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
| `mysqlx_write_timeout` | 60 | レスポンス書き込みの上限秒数 (セッション変数) |
| `mysqlx_enable_hello_notice` | ON | 接続直後に `ServerHello` を送るか |

接続処理に関わるステータス変数 (定義は [status_variables.cc](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/plugin/x/src/variables/status_variables.cc#L376-L415))

- 接続: `Mysqlx_connections_accepted` / `Mysqlx_connections_closed` / `Mysqlx_connections_rejected` / `Mysqlx_connection_errors` / `Mysqlx_connection_accept_errors` / `Mysqlx_aborted_clients`
- セッション: `Mysqlx_sessions` / `Mysqlx_sessions_accepted` / `Mysqlx_sessions_closed` / `Mysqlx_sessions_rejected` / `Mysqlx_sessions_killed` / `Mysqlx_sessions_fatal_error`
- スレッド: `Mysqlx_worker_threads` / `Mysqlx_worker_threads_active`
- 加算のタイミング (本文に出てこないもの)
  - `Mysqlx_connection_accept_errors`: accept に失敗したとき (`Mysqlx_connection_errors` も同時に加算)
  - `Mysqlx_sessions`: 認証成功で加算し、認証済みだったセッションの破棄で減算
  - `Mysqlx_sessions_closed`: 認証中以外の状態から閉じたセッションの破棄時
  - `Mysqlx_sessions_rejected`: 認証に一度も成功せず、失敗があったセッションの破棄時
  - `Mysqlx_sessions_fatal_error`: Fatal エラーを送ったとき
  - 参照:
    - [server.cc の accept 失敗時](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/plugin/x/src/server/server.cc#L361-L366)
    - [session.cc のデストラクタ](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/plugin/x/src/session.cc#L80-L100)
    - [protocol_monitor.cc の on_fatal_error_send](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/plugin/x/src/protocol_monitor.cc#L81-L85)
