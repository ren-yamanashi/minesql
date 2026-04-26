# プロトコル

- MySQL Client (mysql コマンドラインクライアントなど) を使用して MineSQL に接続できることを目指す
- そのためプロトコルは「MySQL プロトコル」をベースにする
  - ただし一部の機能はサポートしていない (例: レプリケーションプロトコル、データ圧縮など)
  - 文字セットは utf8mb4_general_ci のみサポート
- MySQL プロトコルは、MySQL クライアントと MySQL サーバー間で使用される通信規約
- MySQL プロトコルは以下の機能をサポートしている
  - TLS による暗号化
  - データ圧縮
  - クライアントの機能 (能力) や認証データをやり取りするための接続フェーズ (Connection Phase)
  - クライアントからの命令を受け取って実行するためのコマンドフェーズ (Command Phase)

## 参照

- [コネクションのライフサイクル](life-cycle.md)
- [パケット](packet.md)

## MineSQL の実装方針

### Capability Flags

- 初期サポート
  - `CLIENT_PROTOCOL_41`: 4.1 以降のプロトコル
  - `CLIENT_PLUGIN_AUTH`: 認証方式のネゴシエーション
  - `CLIENT_SECURE_CONNECTION`: 4.1 認証プロトコル
  - `CLIENT_DEPRECATE_EOF`: EOF_Packet を使わず OK_Packet で代替
  - `CLIENT_TRANSACTIONS`: トランザクション状態の通知
  - `CLIENT_CONNECT_WITH_DB`: データベース指定での接続 (MineSQL は単一スキーマなので値は無視する)
- 後から追加
  - `CLIENT_SSL`: TLS サポート

### 認証

- 認証方式: `caching_sha2_password` のみサポート
- ユーザーアカウントとパスワードはサーバー側に固定で保持する (ユーザー管理機能は未サポート)
- サーバー起動時から固定ユーザーのパスワードハッシュをキャッシュ済みとして扱い、常に fast path で認証する
- Full authentication (平文パスワードの送信) は TLS サポート時にまとめて対応する

### TLS

- 初期実装では TLS をサポートしない
- 後から `CLIENT_SSL` フラグの対応と TLS ハンドシェイクの処理を追加する
- Full authentication も TLS サポートと同時に対応する

### コマンド

- Text Protocol
  - COM_QUERY: SQL の実行
- Utility Commands
  - COM_QUIT: 接続の終了
  - COM_PING: サーバーの生存確認

### 参考実装

- [go-sql-driver/mysql](https://github.com/go-sql-driver/mysql): Go の MySQL クライアントドライバ。パケット構造の参考
- [TiDB](https://github.com/pingcap/tidb/tree/master/pkg/server): MySQL 互換サーバーの Go 実装
- [Vitess](https://github.com/vitessio/vitess/tree/main/go/mysql): MySQL プロトコルのサーバー側実装
