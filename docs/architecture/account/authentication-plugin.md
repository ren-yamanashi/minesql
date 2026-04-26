# 認証プラグイン

- 参考: https://dev.mysql.com/doc/dev/mysql-server/latest/page_caching_sha2_authentication_exchanges.html

## 概要

- (MySQL プロトコルを使用して) サーバーとクライアントが接続を行う中で「どのようにパスワード (またはそれ以外の認証に使う情報) をやり取りして検証するか」を決めるためのもの
- caching_sha2_password のみをサポートしている

## caching_sha2_password

- SHA-256 ハッシングを実装する認証プラグイン
  - ソルト付きでハッシュ化されるため、同じパスワードでも異なるハッシュ値になる
- `Fast authentication` と `Complete authentication` の 2 つのフェーズで動作する
  - サーバーがメモリ上に対象ユーザーのハッシュ値のキャッシュを保持している場合、クライアントから送信されたスクランブル値 (暗号化されたデータ) を使用して高速に認証を行う (Fast authentication)
  - Fast authentication に失敗した場合、サーバーはクライアントに対して Complete authentication への切り替えを通知する
  - Complete authentication では、安全な接続を介してサーバーにパスワードが送信される
    - サーバーはそのパスワードを `authentication_string` と照合し、照合に成功すると、サーバーはそのアカウントのハッシュ値をキャッシュに保存する (以降はコマンドフェーズへ移行する)

### 用語

| 用語 | 説明 |
| --- | --- |
| Nonce | サーバーが接続ごとに生成する 20 バイトのランダムデータ。初期ハンドシェイクパケットに含まれる |
| Scramble | クライアントがパスワードと Nonce から計算した 32 バイトのデータ。パスワードそのものをネットワーク上に流さずに認証するために使う |
| Hash Entry | サーバーがメモリ上にキャッシュしている `SHA256(SHA256(password))` の値 |
| authentication_string | サーバーが永続化しているソルト付きハッシュ (`$A$005$<salt><hash>` 形式) <br/> SHA-256 を 5000 回イテレーションして生成 |

### 認証フロー全体

```mermaid
flowchart TD
    Start([認証開始])
    --> ServerSendNonce[Server: Nonce をクライアントに送信]
    --> CheckPassword{Client: パスワードの確認}

    CheckPassword -->|空パスワード| EmptyPwServer{Server: アカウントのパスワードが空か？}
    EmptyPwServer -->|空| AuthSuccess1([認証成功])
    EmptyPwServer -->|空でない| AuthFailure1([認証失敗])

    CheckPassword -->|空でないパスワード| FastAuthClient[Client: Scramble を生成してサーバーに送信]
    --> FastAuthServer[Server: Fast Authentication 開始]
    --> CacheCheck{Server: キャッシュにハッシュエントリがあるか？}

    CacheCheck -->|あり| ScrambleVerify{Server: Scramble の検証}
    ScrambleVerify -->|成功| FastAuthOK[Server: Fast Auth Success + OK パケットを送信]
    --> AuthSuccess2([認証成功])

    ScrambleVerify -->|失敗| FullAuthStart
    CacheCheck -->|なし| FullAuthStart

    FullAuthStart[Server: Complete Authentication への切り替えを通知]
    --> ConnCheck{Client: 接続の種類を確認}

    ConnCheck -->|TLS 接続| SendPlaintext[Client: 平文パスワードを送信]

    ConnCheck -->|非 TLS 接続| HasPubKey{Client: RSA 公開鍵を既に持っているか？}
    HasPubKey -->|持っている| EncryptAndSend
    HasPubKey -->|持っていない| ShouldGetKey{Client: サーバーから公開鍵を取得するか？}
    ShouldGetKey -->|Yes| RequestPubKey[Client: 公開鍵リクエストを送信]
    --> ServerSendKey[Server: 公開鍵を送信]
    --> EncryptAndSend
    ShouldGetKey -->|No| ClientError([クライアントエラー])

    EncryptAndSend[Client: 公開鍵でパスワードを暗号化して送信]
    --> ServerDecrypt[Server: 秘密鍵で復号]
    --> ReceivedPassword

    SendPlaintext --> ReceivedPassword[Server: パスワードを受信]
    --> PasswordVerify{Server: パスワードの照合}

    PasswordVerify -->|成功| UpdateCache[Server: キャッシュを更新]
    --> AuthSuccess3([認証成功])
    PasswordVerify -->|失敗| AuthFailure2([認証失敗])
```

### Fast Authentication のシーケンス

```mermaid
sequenceDiagram
    participant C as Client
    participant S as Server

    S->>C: 初期ハンドシェイクパケット (Nonce を含む)
    C->>S: ハンドシェイク応答パケット (Scramble を含む)
    Note over S: キャッシュからハッシュエントリを取得し、スクランブルを検証する
    S->>C: Fast Authentication の成功を通知
    S->>C: OK パケット
    Note over C,S: コマンドフェーズへ移行
```

### Complete Authentication のシーケンス (TLS 接続の場合)

```mermaid
sequenceDiagram
    participant C as Client
    participant S as Server

    S->>C: 初期ハンドシェイクパケット (Nonce を含む)
    C->>S: ハンドシェイク応答パケット (Scramble を含む)
    Note over S: キャッシュにハッシュエントリがない、またはスクランブルの検証に失敗
    S->>C: Complete Authentication への切り替えを通知
    C->>S: 平文パスワード + 0x00 (NUL 終端)
    Note over S: パスワードを authentication_string と照合
    Note over S: 照合成功: ハッシュエントリをキャッシュに保存
    S->>C: OK パケット
    Note over C,S: コマンドフェーズへ移行
```

### Complete Authentication のシーケンス (非 TLS 接続の場合)

```mermaid
sequenceDiagram
    participant C as Client
    participant S as Server

    S->>C: 初期ハンドシェイクパケット (Nonce を含む)
    C->>S: ハンドシェイク応答パケット (Scramble を含む)
    Note over S: キャッシュにハッシュエントリがない、またはスクランブルの検証に失敗
    S->>C: Complete Authentication への切り替えを通知
    C->>S: RSA 公開鍵のリクエスト
    S->>C: RSA 公開鍵を送信
    Note over C: パスワードを Nonce で XOR し、RSA 公開鍵で暗号化
    C->>S: RSA 暗号化されたパスワード
    Note over S: 秘密鍵で復号し、authentication_string と照合
    Note over S: 照合成功: ハッシュエントリをキャッシュに保存
    S->>C: OK パケット
    Note over C,S: コマンドフェーズへ移行
```

### Scramble の計算方法

#### クライアント側の計算

1. `stage1 = SHA256(password)`
2. `stage2 = SHA256(stage1)` (= `SHA256(SHA256(password))`)
3. `digest = SHA256(stage2 || nonce)` (= `SHA256(SHA256(SHA256(password)) || nonce)`)
4. `scramble = XOR(stage1, digest)`

#### サーバー側の検証 (Fast authentication)

1. サーバーは `cached_hash = SHA256(SHA256(password))` をキャッシュとして保持している
2. `expected = SHA256(cached_hash || nonce)` を計算
3. `candidate_stage1 = XOR(client_scramble, expected)` で `SHA256(password)` を復元
4. `candidate_stage2 = SHA256(candidate_stage1)` を計算
5. `candidate_stage2 == cached_hash` なら認証成功

#### サーバー側の検証 (Complete authentication)

- Fast Authentication に失敗した場合、または Hash Entry のキャッシュがない場合に使用する
- TLS 接続上でのみサポートする (非 TLS の RSA 公開鍵暗号化パスはサポートしない)
- フローは以下の通り
  1. サーバーが AuthMoreData パケット (0x04 = perform full auth) を送信
  2. クライアントが平文パスワード + NUL 終端を送信 (TLS により暗号化されている)
  3. サーバーがパスワードを受信し、authentication_string と照合
  4. 照合成功: Hash Entry をキャッシュに保存し、OK パケットを送信
  5. 照合失敗: ERR パケットを送信

#### ソルト付きハッシュ

- authentication_string は MySQL 互換のソルト付きハッシュ (SHA-crypt: SHA-256 を 5000 回イテレーション + ソルト) で永続化する
- Fast Authentication 用のキャッシュ (`SHA256(SHA256(password))`) は別の値であり、ソルト付きハッシュから復元することはできない
- そのため、サーバー起動後の初回接続時は必ず Complete Authentication が発生する
  1. ハッシュエントリのキャッシュが空のため、Fast Authentication は失敗する
  2. Complete Authentication で平文パスワードを受信する (TLS により暗号化されている)
  3. 平文パスワードからソルト付きハッシュと照合する
  4. 照合成功時に `SHA256(SHA256(password))` を計算してキャッシュに保存する
  5. 以降の接続では Fast Authentication が使用される
- ALTER USER でパスワードを変更した場合、キャッシュはクリアされる (次回接続時に Complete Authentication が発生する)
