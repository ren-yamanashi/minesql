package table

import (
	"minesql/internal/storage/access/btree"
	"minesql/internal/storage/access/btree/node"
	"minesql/internal/storage/bufferpool"
	"minesql/internal/storage/disk"
)

type UniqueIndex struct {
	MetaPageId disk.PageId
	// セカンダリキーに含めるカラムを指定
	SecondaryKey uint
}

func NewUniqueIndex(metaPageId disk.PageId, secondaryKey uint) *UniqueIndex {
	return &UniqueIndex{
		MetaPageId:   metaPageId,
		SecondaryKey: secondaryKey,
	}
}

// 空のユニークインデックスを新規作成する
func (ui *UniqueIndex) Create(bpm *bufferpool.BufferPoolManager) error {
	btr, err := btree.CreateBTree(bpm)
	if err != nil {
		return err
	}
	ui.MetaPageId = btr.MetaPageId
	return nil
}

func (ui *UniqueIndex) Insert(bpm *bufferpool.BufferPoolManager, primaryKey []uint8, record [][]byte) error {
	btr := btree.NewBTree(ui.MetaPageId)
	var secondaryKey []byte

	// セカンダリキーをエンコード
	Encode([][]byte{record[ui.SecondaryKey]}, &secondaryKey)

	// B+Tree に挿入
	return btr.Insert(bpm, node.Pair{
		Key:   secondaryKey,
		Value: primaryKey,
	})
}
