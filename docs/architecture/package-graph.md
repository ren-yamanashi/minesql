# パッケージ依存関係グラフ

`internal/` 配下のパッケージ間の依存関係を示す。テストコードの依存は含まない。

## 全体図

```mermaid
graph TD
    server --> ast
    server --> executor
    server --> parser
    server --> planner
    server --> storage/engine

    planner --> ast
    planner --> executor
    planner --> storage/engine

    executor --> storage/engine

    parser --> ast

    storage/engine --> config
    storage/engine --> storage/access
    storage/engine --> storage/buffer
    storage/engine --> storage/catalog
    storage/engine --> storage/file
    storage/engine --> storage/page
    storage/engine --> storage/statistics
    storage/engine --> storage/transaction

    storage/statistics --> storage/access
    storage/statistics --> storage/buffer
    storage/statistics --> storage/catalog

    storage/catalog --> encode
    storage/catalog --> storage/access
    storage/catalog --> storage/btree
    storage/catalog --> storage/btree/node
    storage/catalog --> storage/buffer
    storage/catalog --> storage/page

    storage/access --> encode
    storage/access --> storage/btree
    storage/access --> storage/btree/node
    storage/access --> storage/buffer
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
        storage/engine["storage/engine (handler)"]
        storage/statistics
        storage/catalog
        storage/transaction
        storage/access
        storage/btree
        storage/btree/node
        storage/buffer
        storage/file
        storage/page
    end

    subgraph "共通"
        ast
        config
        encode
        client
    end

    server --> planner & parser & executor & storage/engine & ast
    planner --> executor & storage/engine & ast
    executor --> storage/engine

    storage/engine --> storage/access & storage/catalog & storage/statistics & storage/transaction & storage/buffer & storage/file & storage/page & config
    storage/statistics --> storage/access & storage/catalog & storage/buffer
    storage/catalog --> storage/access & storage/btree & storage/btree/node & storage/buffer & storage/page & encode
    storage/transaction --> storage/buffer
    storage/access --> storage/btree & storage/btree/node & storage/buffer & storage/page & encode
    storage/btree --> storage/btree/node & storage/buffer & storage/page
    storage/btree/node --> storage/page
    storage/buffer --> storage/file & storage/page
    storage/file --> storage/page
```

## MySQL InnoDB との対応

| MySQL InnoDB (`storage/innobase/`) | minesql (`internal/storage/`) |
|---|---|
| `handler/` (ha_innodb.cc) | `engine/` |
| `row/` | `access/` |
| `btr/` | `btree/` |
| `buf/` | `buffer/` |
| `dict/` | `catalog/` |
| `dict/dict0stats.cc` | `statistics/` |
| `fil/` | `file/` |
| `page/` | `page/` |
| `trx/` | `transaction/` |

## 依存の少ないパッケージ (リーフ)

依存先がないパッケージ:

- `ast`
- `client`
- `config`
- `encode`
- `storage/page`

## storage 外からのアクセスルール

`storage/` 外のパッケージ (`server`, `planner`, `executor`) は `storage/engine` のみを参照する。`storage/` 内の他のパッケージ (`access`, `catalog`, `btree` 等) を直接参照しない。

```
server  ──→ storage/engine ──→ storage/* 内部
planner ──→ storage/engine ──→ storage/* 内部
executor ──→ storage/engine ──→ storage/* 内部
```
