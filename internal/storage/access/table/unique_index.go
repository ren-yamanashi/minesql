package table

import (
	"minesql/internal/storage/access/btree"
	"minesql/internal/storage/access/btree/node"
	"minesql/internal/storage/bufferpool"
	"minesql/internal/storage/disk"
)

type UniqueIndex struct {
	MetaPageId disk.OldPageId
	// セカンダリキーに含めるカラムを指定
	SecondaryKey uint
}

func NewUniqueIndex(metaPageId disk.OldPageId, secondaryKey uint) *UniqueIndex {
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

// ユニークインデックスに行を挿入する
// value はプライマリキー (primaryKey に指定された値) になるため、エンコードせずそのまま格納する
func (ui *UniqueIndex) Insert(bpm *bufferpool.BufferPoolManager, primaryKey []uint8, record [][]byte) error {
	btr := btree.NewBTree(ui.MetaPageId)
	var secondaryKey []byte

	// セカンダリキーをエンコード
	Encode([][]byte{record[ui.SecondaryKey]}, &secondaryKey)

	// B+Tree に挿入
	return btr.Insert(bpm, node.NewPair(secondaryKey, primaryKey))
}
