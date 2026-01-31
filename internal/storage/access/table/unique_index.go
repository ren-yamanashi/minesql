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
	// インデックスを構成するカラム名
	ColName string
	// インデックスの内容が入っている B+Tree のメタページの ID
	MetaPageId page.PageId
	// セカンダリキーに含めるカラムのインデックス (0 始まりの列番号)
	SecondaryKeyIdx uint
}

func NewUniqueIndex(name string, colName string, secondaryKeyIdx uint) *UniqueIndex {
	return &UniqueIndex{
		Name:         name,
		ColName:      colName,
		MetaPageId:   page.INVALID_PAGE_ID, // 初期化時には無効なページIDを設定 (Create 時に設定される)
		SecondaryKeyIdx: secondaryKeyIdx,
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
	Encode([][]byte{record[ui.SecondaryKeyIdx]}, &secondaryKey)

	// B+Tree に挿入
	return btr.Insert(bpm, node.NewPair(secondaryKey, primaryKey))
}
