# アーキテクチャ

## 概要 (minesql server)

- [サーバー](./server.md)
- [パーサー](./parser.md)
- [プランナー](./planner.md)
- [エグゼキュータ](./executor.md)
- [エンジン](./engine.md)
- [カタログ](./catalog.md)
- [アクセスメソッド](./access.md)
- [ストレージ](./storage)

## アーキテクチャ図

```mermaid
graph TD
    client[クライアント]

    server[サーバー]
    parser[パーサー]
    planner[プランナー]
    executor[エグゼキュータ]
    storage[ストレージ]

    client ----> server
    server ----> client

    server --SQL--> parser
    parser --AST--> planner
    planner --実行計画--> executor
    executor --アクセス--> storage
    storage --データ--> server
```

※エンジン (`internal/engine`) はサーバー起動時にバッファプールとカタログを初期化するパッケージ  
図ではクエリ処理の流れを優先し省略している
