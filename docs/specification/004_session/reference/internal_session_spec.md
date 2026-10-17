# 内部セッションの詳細仕様 (MySQL との対応)

- [internal_session.md](../internal_session.md) の論理モデルに対する、MySQL のソースとの対応
- MySQL 8.4 (commit `aa461240`) の `sql/sql_parse.cc`、`sql/sql_class.h`、`sql/srv_session.cc`、`plugin/x/src/sql_data_context.cc` を参照
- 実装時の参照資料で、論理モデルと食い違う場合は論理モデルを正とする

## 論理モデルの主張とソースの対応

### [internal_session.md](../internal_session.md) より

- X Plugin は接続ごとに内部セッションを開き、ステートメントを classic protocol のコマンドの形で渡す
  - [srv_session.h の説明 (THD と診断領域を 1 つに包む)](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/sql/srv_session.h#L44-L49)
  - [sql_data_context.cc の srv_session_open](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/plugin/x/src/sql_data_context.cc#L101)、[command_service_run_command の呼び出し](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/plugin/x/src/sql_data_context.cc#L198)
  - [command_service.cc の command_service_run_command](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/sql/command_service.cc#L75)、[srv_session.cc の Srv_session::execute_command](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/sql/srv_session.cc#L1120) から [dispatch_command](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/sql/srv_session.cc#L1183)
- 内部セッションの入口は「コマンドの受付」と「ステートメントの実行」の 2 段
  - [sql_parse.cc の dispatch_command](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/sql/sql_parse.cc#L1741)、[COM_QUERY の分岐](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/sql/sql_parse.cc#L2079-L2136)
  - [dispatch_sql_command (解析)](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/sql/sql_parse.cc#L5275)、[mysql_execute_command の呼び出し](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/sql/sql_parse.cc#L5406)
  - [mysql_execute_command](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/sql/sql_parse.cc#L2909)
- 診断はステートメントごとにリセットし、終わりに後始末する
  - [THD::reset_for_next_command](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/sql/sql_parse.cc#L5171)、[THD::cleanup_after_query](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/sql/sql_class.cc#L1794)
  - [sql_error.h の Diagnostics_area (現在のステートメントの状態)](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/sql/sql_error.h#L264)
- 暗黙のコミットは入口が種別のフラグで判定し、前と後で行う
  - [stmt_causes_implicit_commit](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/sql/sql_parse.cc#L404)
  - [前のコミット](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/sql/sql_parse.cc#L3221-L3238)、[後のコミット](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/sql/sql_parse.cc#L4993)
- SQL コマンドを持つステートメントはその SQL コマンドを実行し、持たないステートメントは種別の分岐から直接処理を呼ぶ
  - [m_sql_cmd->execute の呼び出し](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/sql/sql_parse.cc#L3672)
  - [SQLCOM_KILL](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/sql/sql_parse.cc#L4227-L4245)、[SQLCOM_BEGIN / SQLCOM_COMMIT](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/sql/sql_parse.cc#L4260-L4273)、[SQLCOM_CREATE_USER](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/sql/sql_parse.cc#L3956)、[SQLCOM_GRANT](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/sql/sql_parse.cc#L4006)
- 実行コンテキストが持つもの
  - [sql_class.h の Security_context](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/sql/sql_class.h#L1333)、[デフォルトスキーマ m_db](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/sql/sql_class.h#L1034)、[トランザクション](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/sql/sql_class.h#L2147)、[診断領域](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/sql/sql_class.h#L256)、[kill の状態](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/sql/sql_class.h#L2751)
- kill は状態を立てるだけで、実行中の処理が確認点で見る
  - [sql_executor.cc の killed の確認](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/sql/sql_executor.cc#L3561)
- ステートメントの終わりに取得したディクショナリオブジェクトを解放する
  - [dictionary_client.h の Auto_releaser](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/sql/dd/cache/dictionary_client.h#L177)
