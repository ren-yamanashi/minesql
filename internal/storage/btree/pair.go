package btree

import "minesql/internal/storage/btree/node"

// node パッケージの Pair 型を再エクスポートする
// 外部パッケージが btree/node を直接 import しなくて済むようにする

// Pair は B+Tree のノード内で使用される key-value ペアを表す
type Pair = node.Pair

// NewPair は指定されたキーと値から新しい key-value ペアを作成する
var NewPair = node.NewPair
