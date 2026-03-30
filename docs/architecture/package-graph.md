# パッケージ依存関係グラフ

`internal/` 配下のパッケージ間の依存関係を示す。テストコードの依存は含まない。

## 全体図

```mermaid
graph TD
    server --> ast
    server --> executor
    server --> parser
    server --> planner
    server --> storage/handler

    planner --> ast
    planner --> executor
    planner --> storage/handler

    executor --> storage/handler

    parser --> ast

    storage/handler --> config
    storage/handler --> storage/access
    storage/handler --> storage/buffer
    storage/handler --> storage/dictionary
    storage/handler --> storage/file
    storage/handler --> storage/page
    storage/handler --> storage/transaction

    storage/dictionary --> storage/access
    storage/dictionary --> storage/btree
    storage/dictionary --> storage/btree/node
    storage/dictionary --> storage/buffer
    storage/dictionary --> storage/encode
    storage/dictionary --> storage/page

    storage/access --> storage/btree
    storage/access --> storage/btree/node
    storage/access --> storage/buffer
    storage/access --> storage/encode
    storage/access --> storage/page

    storage/transaction --> storage/buffer

    storage/btree --> storage/btree/node
    storage/btree --> storage/buffer
    storage/btree --> storage/page

    storage/btree/node --> storage/page

    storage/buffer --> storage/file
    storage/buffer --> storage/page

    storage/file --> storage/page
```

## レイヤー構造

```mermaid
graph TD
    subgraph "Layer 3: サーバー"
        server
    end

    subgraph "Layer 2: クエリ処理"
        planner
        parser
        executor
    end

    subgraph "Layer 1: ストレージエンジン (= storage/)"
        storage/handler["storage/handler (エントリポイント)"]
        storage/dictionary
        storage/transaction
        storage/access
        storage/encode
        storage/btree
        storage/btree/node
        storage/buffer
        storage/file
        storage/page
    end

    subgraph "共通"
        ast
        config
        client
    end

    server --> planner & parser & executor & storage/handler & ast
    planner --> executor & storage/handler & ast
    executor --> storage/handler

    storage/handler --> storage/access & storage/dictionary & storage/transaction & storage/buffer & storage/file & storage/page & config
    storage/dictionary --> storage/access & storage/btree & storage/btree/node & storage/buffer & storage/encode & storage/page
    storage/access --> storage/btree & storage/btree/node & storage/buffer & storage/encode & storage/page
    storage/transaction --> storage/buffer
    storage/btree --> storage/btree/node & storage/buffer & storage/page
    storage/btree/node --> storage/page
    storage/buffer --> storage/file & storage/page
    storage/file --> storage/page
```

## MySQL InnoDB との対応

| MySQL InnoDB (`storage/innobase/`) | minesql (`internal/storage/`) |
|---|---|
| `handler/` (ha_innodb.cc) | `handler/` |
| `row/` | `access/` |
| `btr/` | `btree/` |
| `buf/` | `buffer/` |
| `dict/` | `dictionary/` |
| `rem/` | `encode/` |
| `fil/` | `file/` |
| `page/` | `page/` |
| `trx/` | `transaction/` |
