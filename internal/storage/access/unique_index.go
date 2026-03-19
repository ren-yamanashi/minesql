package access

import (
	"minesql/internal/storage/btree"
	"minesql/internal/storage/btree/node"
	"minesql/internal/storage/bufferpool"
	"minesql/internal/storage/page"
)

// UniqueIndexAccessMethod はユニークインデックスへのアクセスを提供する
type UniqueIndexAccessMethod struct {
	Name            string      // インデックス名
	ColName         string      // インデックスを構成するカラム名
	MetaPageId      page.PageId // インデックスの内容が入っている B+Tree のメタページの ID
	SecondaryKeyIdx uint16      // セカンダリキーに含めるカラムのインデックス (0 始まりの列番号)
}

func NewUniqueIndexAccessMethod(name string, colName string, metaPageId page.PageId, secondaryKeyIdx uint16) *UniqueIndexAccessMethod {
	return &UniqueIndexAccessMethod{
		Name:            name,
		ColName:         colName,
		MetaPageId:      metaPageId,
		SecondaryKeyIdx: secondaryKeyIdx,
	}
}

// Create は空のユニークインデックスを新規作成する
func (ui *UniqueIndexAccessMethod) Create(bp *bufferpool.BufferPool) error {
	btr, err := btree.CreateBPlusTree(bp, ui.MetaPageId)
	if err != nil {
		return err
	}
	ui.MetaPageId = btr.MetaPageId
	return nil
}

// Insert はユニークインデックスに行を挿入する
//
// value はプライマリキー (primaryKey に指定された値) になるため、エンコードせずそのまま格納する
func (ui *UniqueIndexAccessMethod) Insert(bp *bufferpool.BufferPool, primaryKey []uint8, record [][]byte) error {
	btr := btree.NewBPlusTree(ui.MetaPageId)
	var secondaryKey []byte

	// セカンダリキーをエンコード
	Encode([][]byte{record[ui.SecondaryKeyIdx]}, &secondaryKey)

	// B+Tree に挿入
	return btr.Insert(bp, node.NewPair(secondaryKey, primaryKey))
}

// Delete はユニークインデックスから行を削除する
func (ui *UniqueIndexAccessMethod) Delete(bp *bufferpool.BufferPool, record [][]byte) error {
	btr := btree.NewBPlusTree(ui.MetaPageId)
	var secondaryKey []byte

	// セカンダリキーをエンコード
	Encode([][]byte{record[ui.SecondaryKeyIdx]}, &secondaryKey)

	// B+Tree から削除
	return btr.Delete(bp, secondaryKey)
}

// Update はユニークインデックスから行を更新する
func (ui *UniqueIndexAccessMethod) Update(bp *bufferpool.BufferPool, oldRecord [][]byte, newRecord [][]byte, primaryKey []byte) error {
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
