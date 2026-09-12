# 認証 (X Plugin)

- コネクションハンドラーのうち、接続をセッションとして使える状態にする「認証」の部分の仕様
  - 接続の状態遷移と、認証メッセージがセッションに届くまでの経路は [connection_handler.md](./connection_handler.md) を参照
  - メッセージの形式 (`AuthenticateStart` / `AuthenticateContinue` / `AuthenticateOk`) は [protocol/message_spec.md の認証](../protocol/message_spec.md#認証-セッション確立) を参照
- 認証は MySQL でも接続処理の一部として (接続ごとのスレッド上で、セッション確立の段階で) 実行されるが、メカニズムの中身は状態管理とは別の部品が担うため、文書を分けている
- 本文の主張に対応する MySQL のソースは [authentication_spec.md の「論理モデルの主張とソースの対応」](./reference/authentication_spec.md#論理モデルの主張とソースの対応) にまとめる

## 要件

- `AuthenticateStart` で指定された認証メカニズム (`MYSQL41` / `PLAIN` / `SHA256_MEMORY`) で認証し、成功なら `AuthenticateOk`、失敗なら `Error` (`ER_ACCESS_DENIED_ERROR`) を返す ([ADR-0015](../adr/0015.認証の方式.md))
- 使えるメカニズムは接続の種類で変わる
  - 安全でない接続 (TLS なしの TCP): `MYSQL41` と `SHA256_MEMORY`
  - 安全な接続 (TLS または Unix ソケット): 上記に加えて `PLAIN`
  - 対応しないメカニズム名 (安全でない接続での `PLAIN` を含む) は FATAL の `Error` (`ER_NOT_SUPPORTED_AUTH_MODE`) で、接続は閉じられる
- 成功時は `SessionStateChanged` (`CLIENT_ID_ASSIGNED`) の Notice を送ってから `AuthenticateOk` を返し、セッションが利用可能になる
- 失敗は 1 セッションにつき 3 回まで試せる
  - 3 回目の失敗の `Error` は FATAL になり、サーバーは認証を打ち切って接続を閉じる (途中で別のメカニズムに切り替えて試すことはできる)
- 認証に成功した接続は、その利用者としてコマンドを実行する (内部セッションの実行ユーザーがその利用者になる)
- パスワードそのものを送るのは `PLAIN` だけで、`MYSQL41` と `SHA256_MEMORY` はサーバーが送った salt とパスワードから計算した値を送る (安全でない接続で平文のパスワードが流れない)

- `PLAIN` は成功時に SHA256 パスワードキャッシュにその利用者の値を入れ、`SHA256_MEMORY` はキャッシュに値がなければ失敗する
- `MYSQL41` は `mysql_native_password` のアカウント専用で、`caching_sha2_password` のアカウントにはチャレンジを返したうえで access denied を返す (MySQL と同じ応答)
  - したがって TLS なしの TCP では初回に `PLAIN` を使えず、TLS 接続か Unix ソケットで一度 `PLAIN` を通したあとに `SHA256_MEMORY` で認証する (MySQL 8.4 の既定と同じ)
- アカウントの認証プラグインは `caching_sha2_password` のみ
  - 認証文字列は `$A$005$` + 20 バイトの salt + ダイジェスト (SHA256 を 5000 回反復) の形式
  - `SHA256_MEMORY` の応答は `XOR(SHA256(password), SHA256(SHA256(SHA256(password)) + nonce))` で、サーバーはキャッシュにある `SHA256(SHA256(password))` からこれを検証する
- アカウント情報はシステムスキーマ `mysql` の `user` 表に置き、起動時の bootstrap でこの表と初期アカウントを作る ([ADR-0014](../adr/0014.スキーマを持つ.md))
- 照合は、内部セッションの実行ユーザーをシステムユーザーにして `mysql.user` を検索し、認証側で照合の計算を行い、成功したら SQL 層の API で実行ユーザーを利用者に切り替える (MySQL と同じ手順で、詳細は [構成要素](#構成要素) と [処理の流れ](#処理の流れ) を参照)
- 扱うアカウントは初期アカウントのみで、ホストは `%` (user@host のパターン照合は行わない)

## 責務

### 担うこと

- メカニズムの選択と、メカニズムごとのやり取りの進行 (チャレンジの生成、応答の受け取り)
- 資格情報 (既定スキーマ、ユーザー名、パスワードまたはその計算値) の取り出しと、アカウント情報との照合
- 成功時の内部セッションの実行ユーザーの確定 (どの利用者として SQL を実行するか)
- 失敗回数の管理と、上限に達したときの打ち切り

### 担わないこと

- 認証メッセージの受け渡しと接続の状態遷移 -> [connection_handler.md](./connection_handler.md)
- アカウントの作成・変更・削除 -> 対象外 ([issue #120](https://github.com/ren-yamanashi/minesql/issues/120) の「アカウント作成 (初期アカウントのみサポート)」)
- アカウントのロック、パスワードの期限、`offline_mode`、TLS 要件 (`require_secure_transport` とアカウントの `ssl_type`) の検査 -> 対象外 (アカウント管理・サーバーモード・TLS の機能に付随するため)
- 認証後の権限の判定と ACL キャッシュ (user@host のパターン照合を含む) -> 対象外 (権限を入れるときの拡張点)

## 構成要素

- 認証ハンドラ (Authentication): `AuthenticateStart` のたびにメカニズムに対応するものが 1 つ作られ、成功または失敗で消える
  - 3 つのメカニズムは 2 種類の実装に対応する
    - チャレンジレスポンス型 (`MYSQL41`、`SHA256_MEMORY`): サーバーが 20 バイトの salt を送り、クライアントはパスワードのハッシュと salt から計算した値を返す
    - 1 往復型 (`PLAIN`): クライアントが最初のメッセージに資格情報をそのまま入れる
- 認証ハンドラの一覧 (Authentication_container): サーバーに 1 つ、メカニズム名と接続の種類 (安全かどうか) から認証ハンドラを作る
- アカウント照合 (Account_verification_handler): 資格情報を「既定スキーマ \0 ユーザー名 \0 パスワード (または計算値)」に分解し、アカウント情報を取り出して照合する
  - 照合の計算はアカウントの認証プラグインの種類ごとの検証器が行う (`mysql_native_password` 用、`caching_sha2_password` 用、キャッシュ用など)
  - アカウント情報は内部セッションで `mysql.user` を検索して取る (認証文字列、プラグイン名、ロック状態、パスワードの期限、TLS の要件)
- SHA256 パスワードキャッシュ: サーバーに 1 つ、`SHA256_MEMORY` の照合に使う値 (パスワードの SHA256 の SHA256) を利用者ごとに保持する
  - 値が入るのは、その利用者が平文で照合できるメカニズム (`PLAIN` など) で一度成功したとき
  - 資格情報の変更・アカウントの改名や削除・`FLUSH PRIVILEGES` で消える (SQL 層の監査イベントを受けて消す)
  - したがって `SHA256_MEMORY` は「平文の認証を一度通した後の 2 回目以降」を速く安全にするためのメカニズムで、キャッシュが空なら失敗する
- 内部セッションの実行ユーザー (security context): 照合の間はシステムユーザー (`mysql.session`@`localhost`) として動き、成功したら認証した利用者に切り替え、既定スキーマの指定があればそれも設定する

## 処理の流れ

```mermaid
sequenceDiagram
    participant C as クライアントプログラム
    participant S as セッション
    participant A as 認証ハンドラ
    participant V as アカウント照合
    participant I as 内部セッション (SQL 層)
    C->>S: AuthenticateStart (メカニズム名、PLAIN なら資格情報も)
    S->>A: メカニズムに対応する認証ハンドラを作って開始
    opt チャレンジレスポンス型 (MYSQL41 / SHA256_MEMORY)
        A-->>C: AuthenticateContinue (20 バイトの salt)
        C->>S: AuthenticateContinue (既定スキーマ \0 ユーザー名 \0 計算値)
        S->>A: 応答を渡す
    end
    A->>V: 資格情報を渡して照合を依頼
    V->>I: システムユーザーとして mysql.user を検索
    I-->>V: アカウント情報 (認証文字列、プラグイン、ロック、期限、TLS 要件)
    V->>V: 検証器で照合し、ロックや期限などを検査
    V->>I: 実行ユーザーを認証した利用者に切り替える
    V-->>A: 結果
    A-->>S: 成功 / 失敗
    alt 成功
        S-->>C: Notice: SessionStateChanged (CLIENT_ID_ASSIGNED)
        S-->>C: AuthenticateOk
    else 失敗 (3 回目までは ERROR、3 回目は FATAL)
        S-->>C: Error (ER_ACCESS_DENIED_ERROR)
    end
```

- メカニズムごとの違い
  - `MYSQL41`: salt に対する応答は `mysql_native_password` の方式 (SHA1 に基づく) で計算し、`mysql.user` の認証文字列と突き合わせる
  - `SHA256_MEMORY`: 応答は SHA256 に基づいて計算し、`mysql.user` ではなく SHA256 パスワードキャッシュの値と突き合わせる
  - `PLAIN`: 最初のメッセージの資格情報 (パスワードは平文) を、アカウントの認証プラグインに応じた検証器で突き合わせ、成功したら SHA256 パスワードキャッシュにも登録する
- 照合の検査項目 (MySQL): パスワードの一致のほかに、アカウントのロック、パスワードの期限切れ (期限切れは接続を許すが SQL を制限する「サンドボックス」になりうる)、TLS の要件 (`require_secure_transport` とアカウントの `ssl_type`) を見る

## 層ごとの分担

認証ハンドラとアカウント照合は独立した層ではなく、コネクションハンドラー (connection/) の中でセッションが使う部品 (MySQL でも X Plugin の中にある)

| 層 | 認証で担うこと |
| --- | --- |
| コネクションハンドラー: 接続とセッション | 認証メッセージの受け渡し、接続とセッションの状態遷移、失敗回数の管理と打ち切り |
| コネクションハンドラー: 認証ハンドラとアカウント照合 (この文書) | メカニズムの進行 (salt の生成と応答の受け取り)、資格情報の分解、照合の計算、SHA256 パスワードキャッシュの更新 |
| SQL 層 (内部セッション) | システムユーザーとしての `mysql.user` の SELECT の実行、実行ユーザーの利用者への切り替えと既定スキーマの設定 |
| データディクショナリとストレージ | システムスキーマ `mysql` と `user` 表の保持 (普通の表として)、起動時の bootstrap |
