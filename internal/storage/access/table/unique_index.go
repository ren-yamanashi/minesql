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
	SecondaryKeyIdx uint16
}

func NewUniqueIndex(name string, colName string, secondaryKeyIdx uint16) *UniqueIndex {
	return &UniqueIndex{
		Name:            name,
		ColName:         colName,
		MetaPageId:      page.INVALID_PAGE_ID, // 初期化時には無効なページIDを設定 (Create 時に設定される)
		SecondaryKeyIdx: secondaryKeyIdx,
	}
}

// 空のユニークインデックスを新規作成する
// 事前に MetaPageId が設定されている必要がある
func (ui *UniqueIndex) Create(bp *bufferpool.BufferPool, metaPageId page.PageId) error {
	ui.MetaPageId = metaPageId
	btr, err := btree.CreateBPlusTree(bp, metaPageId)
	if err != nil {
		return err
	}
	ui.MetaPageId = btr.MetaPageId
	return nil
}

// ユニークインデックスに行を挿入する
// value はプライマリキー (primaryKey に指定された値) になるため、エンコードせずそのまま格納する
func (ui *UniqueIndex) Insert(bp *bufferpool.BufferPool, primaryKey []uint8, record [][]byte) error {
	btr := btree.NewBPlusTree(ui.MetaPageId)
	var secondaryKey []byte

	// セカンダリキーをエンコード
	Encode([][]byte{record[ui.SecondaryKeyIdx]}, &secondaryKey)

	// B+Tree に挿入
	return btr.Insert(bp, node.NewPair(secondaryKey, primaryKey))
}

// ユニークインデックスから行を削除する
func (ui *UniqueIndex) Delete(bp *bufferpool.BufferPool, record [][]byte) error {
	btr := btree.NewBPlusTree(ui.MetaPageId)
	var secondaryKey []byte

	// セカンダリキーをエンコード
	Encode([][]byte{record[ui.SecondaryKeyIdx]}, &secondaryKey)

	// B+Tree から削除
	return btr.Delete(bp, secondaryKey)
}

// ユニークインデックスから行を更新する
func (ui *UniqueIndex) Update(bp *bufferpool.BufferPool, oldRecord [][]byte, newRecord [][]byte, primaryKey []byte) error {
	btr := btree.NewBPlusTree(ui.MetaPageId)
	var oldSecondaryKey []byte
	var newSecondaryKey []byte

	// セカンダリキーをエンコード
	Encode([][]byte{oldRecord[ui.SecondaryKeyIdx]}, &oldSecondaryKey)
	Encode([][]byte{newRecord[ui.SecondaryKeyIdx]}, &newSecondaryKey)

	// キーが一致しない場合は、B+Tree から古いキーに該当するペアを削除し、新しいキーに該当するペアを挿入する
	if string(oldSecondaryKey) != string(newSecondaryKey) {
		err := btr.Delete(bp, oldSecondaryKey)
		if err != nil {
			return err
		}
		err = btr.Insert(bp, node.NewPair(newSecondaryKey, primaryKey))
		if err != nil {
			return err
		}
	} else {
		// キーが一致する場合は、B+Tree のペアを更新する
		err := btr.Update(bp, node.NewPair(oldSecondaryKey, primaryKey))
		if err != nil {
			return err
		}
	}
	return nil
}
