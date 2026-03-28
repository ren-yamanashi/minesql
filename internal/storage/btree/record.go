package btree

import "minesql/internal/storage/btree/node"

// MEMO: 外部パッケージが btree/node を直接 import しなくて済むように node パッケージの Record 型を再エクスポートする

// Record は B+Tree のノード内で使用されるレコードを表す
type Record = node.Record

// NewRecord は指定されたヘッダー、キー、非キーフィールドから新しいレコードを作成する
var NewRecord = node.NewRecord
