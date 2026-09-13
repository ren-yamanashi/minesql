# MineSQL 仕様

## 全体像

クライアントからのメッセージは次の順に流れる

```mermaid
flowchart TB
    client["クライアント (MySQL Shell など)"]

    subgraph server["サーバー"]
        conn["コネクションハンドラー"]
        dispatcher["コマンドディスパッチャ"]

        subgraph sqlproc["ステートメントの処理"]
            parser["SQL パーサー"]
            prepare["プリペア"]
            optimizer["オプティマイザ"]
            executor["エグゼキュータ"]
        end

        dict["データディクショナリ"]
        engine[("ストレージエンジン")]
    end

    client -->|"メッセージ (X Protocol)"| conn
    conn -->|"認証後のメッセージ"| dispatcher
    dispatcher -->|"ステートメント"| parser
    parser -->|"実行コマンド (未解決の AST)"| prepare
    prepare -->|"解決済みの AST"| optimizer
    prepare -->|"定義の参照"| dict
    optimizer -->|"実行計画"| executor
    executor -->|"行の読み書き"| engine
    executor -->|"実行結果"| dispatcher
    dispatcher -->|"レスポンス"| conn
    conn -->|"メッセージ (X Protocol)"| client
    dict -.->|"システム表として格納"| engine
```

※プロトコル (X Protocol) は図の箱ではなく、クライアントとサーバーの間で交わすメッセージの規約で、コネクションハンドラーとコマンドディスパッチャが使う

## モジュール

| モジュール | 何をするか | 仕様の場所 |
| --- | --- | --- |
| プロトコル | クライアントとサーバーの間で交わすメッセージの形式と流れの規約 (X Protocol) | [protocol/](./protocol/README.md) |
| コネクションハンドラー | 接続の受付から認証、セッションの確立と切断まで | [connection/](./connection/README.md) |
| コマンドディスパッチャ | メッセージの種別ごとの振り分けと、実行結果のレスポンスとしての返送 | [dispatcher/](./dispatcher/README.md) |
| SQL パーサー | ステートメントの文字列から AST を作り、実行コマンドに包む | [parser/](./parser/README.md) |
| データディクショナリ | スキーマ・テーブル・列・インデックスの定義の保持 | `dictionary/` |
| プリペア | 名前解決と型決定、権限の検査 | `prepare/` |
| オプティマイザ | 実行計画の選択 | `optimizer/` |
| エグゼキュータ | 実行計画の実行と結果の返送 | `executor/` |
| ストレージエンジン | 行とインデックスの永続化、トランザクション、ロック、ログ | `storage/` |

- SQL パーサーからエグゼキュータまで (ステートメントの処理) を「SQL 層」と総称する
