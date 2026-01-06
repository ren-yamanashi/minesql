# Node

- 実装コード
  - [node.go](../../../internal/storage/access/btree/node/node.go)
  - [leaf_node.go]()
  - [internal_node.go]()

### 内部ノード

### リーフノード

以下の構造

|リーフヘッダー (16 バイト)|ノードの種類名 (8 バイト)| ...|

### key-value ペア

ペアのバイト列は以下のように構成される

|key サイズ (4 バイト)|key (可変長)|value (可変長)|
