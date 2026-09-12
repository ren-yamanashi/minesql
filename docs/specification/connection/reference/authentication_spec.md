# 認証の詳細仕様 (X Plugin)

- [authentication.md](../authentication.md) の論理モデルに対する、MySQL のソースとの対応
- MySQL 8.4 (commit `aa461240`) の `plugin/x/src` を参照
- 実装時の参照資料で、論理モデルと食い違う場合は論理モデルを正とする

## 論理モデルの主張とソースの対応

論理モデルの文書から移した、各主張に対応する MySQL のソースへの参照

### [authentication.md](../authentication.md) より

- 使えるメカニズムは接続の種類で変わる
  - [authentication_container.cc のメカニズム登録](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/plugin/x/src/server/authentication_container.cc#L37-L46)
  - [get_auth_handler の接続種別による絞り込み](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/plugin/x/src/server/authentication_container.cc#L49-L68)
  - [session.cc の未対応メカニズムの応答](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/plugin/x/src/session.cc#L155-L162)
- 成功時は `SessionStateChanged` (`CLIENT_ID_ASSIGNED`) の Notice を送ってから `AuthenticateOk` を返し、セッションが利用可能になる
  - [session.cc の on_auth_success](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/plugin/x/src/session.cc#L204-L214)
- 失敗は 1 セッションにつき 3 回まで試せる
  - [session.cc の on_auth_failure_impl](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/plugin/x/src/session.cc#L222-L247)
  - [session.h の k_max_auth_attempts](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/plugin/x/src/session.h#L125)
- アカウントの認証プラグインは `caching_sha2_password` のみ
  - [mysql_native_password.cc のプラグイン宣言 (8.4 では既定で無効)](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/sql/auth/mysql_native_password.cc#L327-L343)
  - [sha2_plain_verification.cc の認証文字列の分解](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/plugin/x/src/sha2_plain_verification.cc#L55-L80)
  - [i_sha2_password_common.h の scramble の形式](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/sql/auth/i_sha2_password_common.h#L96-L97)
- 認証ハンドラ (Authentication): `AuthenticateStart` のたびにメカニズムに対応するものが 1 つ作られ、成功または失敗で消える
  - [auth_challenge_response.h の Sasl_challenge_response_auth (やり取りの説明)](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/plugin/x/src/auth_challenge_response.h#L42-L68)
  - [auth_plain.cc の Sasl_plain_auth](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/plugin/x/src/auth_plain.cc#L49-L62)
- アカウント照合 (Account_verification_handler): 資格情報を「既定スキーマ \0 ユーザー名 \0 パスワード (または計算値)」に分解し、アカウント情報を取り出して照合する
  - [account_verification_handler.cc の authenticate (資格情報の分解)](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/plugin/x/src/account_verification_handler.cc#L39-L70)
  - [verify_account (照合と各種の検査)](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/plugin/x/src/account_verification_handler.cc#L125-L181)
  - [get_account_record (mysql.user の検索)](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/plugin/x/src/account_verification_handler.cc#L183-L244)
- SHA256 パスワードキャッシュ: サーバーに 1 つ、`SHA256_MEMORY` の照合に使う値 (パスワードの SHA256 の SHA256) を利用者ごとに保持する
  - [cache_based_verification.cc の照合](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/plugin/x/src/cache_based_verification.cc#L70-L91)
  - [sha2_plain_verification.cc の成功時のキャッシュ登録](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/plugin/x/src/sha2_plain_verification.cc#L81-L85)
  - [module_cache.cc のキャッシュの消去](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/plugin/x/src/module_cache.cc#L82-L96)
- 内部セッションの身元 (security context): 照合の間はシステムユーザー (`mysql.session`@`localhost`) として動き、成功したら認証した利用者に切り替え、既定スキーマの指定があればそれも設定する
  - [sql_data_context.cc の authenticate_internal](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/plugin/x/src/sql_data_context.cc#L257-L300)
  - [switch_to_user](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/plugin/x/src/sql_data_context.cc#L445-L499)
