package table

import (
	"minesql/internal/storage/access/btree"
	"minesql/internal/storage/access/btree/node"
	"minesql/internal/storage/bufferpool"
	"minesql/internal/storage/page"
)

type UniqueIndex struct {
	// インデックス名
	Name string
	// インデックスの内容が入っている B+Tree のメタページの ID
	MetaPageId page.PageId
	// セカンダリキーに含めるカラムを指定
	SecondaryKey uint
}

func NewUniqueIndex(name string, secondaryKey uint) *UniqueIndex {
	return &UniqueIndex{
		Name:         name,
		MetaPageId:   page.INVALID_PAGE_ID,
		SecondaryKey: secondaryKey,
	}
}

// 空のユニークインデックスを新規作成する
// 事前に MetaPageId が設定されている必要がある
func (ui *UniqueIndex) Create(bpm *bufferpool.BufferPoolManager, metaPageId page.PageId) error {
	ui.MetaPageId = metaPageId
	btr, err := btree.CreateBTree(bpm, metaPageId)
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
