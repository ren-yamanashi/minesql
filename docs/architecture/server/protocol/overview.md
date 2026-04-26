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

- [コネクションのライフサイクル](life-cycle.md): 接続フェーズとコマンドフェーズの詳細
- [パケット](packet.md): パケット構造と各パケットのフィールド定義
- [認証プラグイン](../../account/authentication-plugin.md): caching_sha2_password の認証フロー

## 参考実装

- [go-sql-driver/mysql](https://github.com/go-sql-driver/mysql): Go の MySQL クライアントドライバ。パケット構造の参考
- [TiDB](https://github.com/pingcap/tidb/tree/master/pkg/server): MySQL 互換サーバーの Go 実装
- [Vitess](https://github.com/vitessio/vitess/tree/main/go/mysql): MySQL プロトコルのサーバー側実装
