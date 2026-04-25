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
