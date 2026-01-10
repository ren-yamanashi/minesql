package table

import (
	"minesql/internal/storage/access/btree"
	"minesql/internal/storage/bufferpool"
	"minesql/internal/storage/disk"
)

type SimpleTable struct {
	// テーブルの内容が入っている B+Tree のメタページの ID
	MetaPageId disk.PageId
	// 左からいくつ目の列がプライマリキーなのかを表す数値
	PrimaryKeyIndex int
}

// 空のテーブルを新規作成する
func (t *SimpleTable) Create(bpm *bufferpool.BufferPoolManager) error {
	tree, err := btree.CreateBTree(bpm)
	if err != nil {
		return err
	}
	t.MetaPageId = tree.MetaPageId
	return nil
}


// テーブルに行を挿入する
// func (t *SimpleTable) Insert(bpm *bufferpool.BufferPoolManager, primaryKey []uint8, record [][]byte) error {
// 	btree := btree.NewBTree(t.MetaPageId)

// 	// キーをエンコード
// 	var encodedKey []byte

// }