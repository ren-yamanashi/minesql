package access

import (
	"bytes"
	"errors"
	"minesql/internal/storage/btree"
	"minesql/internal/storage/btree/node"
	"minesql/internal/storage/buffer"
	"minesql/internal/storage/encode"
	"minesql/internal/storage/page"
)

// UniqueIndex はユニークインデックスへのアクセスを提供する
type UniqueIndex struct {
	Name       string      // インデックス名
	ColName    string      // インデックスを構成するカラム名
	MetaPageId page.PageId // インデックスの内容が入っている B+Tree のメタページの ID
	UkIdx      uint16      // ユニークキーに含めるカラムのインデックス (0 始まりの列番号)
	PkCount    uint8       // PK のカラム数 (key からユニークキーと PK を分離するために必要)
}

func NewUniqueIndex(name string, colName string, metaPageId page.PageId, ukIdx uint16, pkCount uint8) *UniqueIndex {
	return &UniqueIndex{
		Name:       name,
		ColName:    colName,
		MetaPageId: metaPageId,
		UkIdx:      ukIdx,
		PkCount:    pkCount,
	}
}

// Search は指定した検索モードでインデックスを検索し、SecondaryIndexIterator を返す
func (ui *UniqueIndex) Search(bp *buffer.BufferPool, table *Table, mode RecordSearchMode) (*UniqueIndexIterator, error) {
	indexBTree := btree.NewBTree(ui.MetaPageId)
	indexIter, err := indexBTree.Search(bp, mode.encode())
	if err != nil {
		return nil, err
	}
	tableBTree := btree.NewBTree(table.MetaPageId)
	return newUniqueIndexIterator(indexIter, tableBTree, bp, ui.PkCount), nil
}

// Create は空のユニークインデックスを新規作成する
func (ui *UniqueIndex) Create(bp *buffer.BufferPool) error {
	btr, err := btree.CreateBTree(bp, ui.MetaPageId)
	if err != nil {
		return err
	}
	ui.MetaPageId = btr.MetaPageId
	return nil
}

// Insert はユニークインデックスに行を挿入する
//
// Key = concat(encodedSecondaryKey, encodedPK), NonKey = nil, Header = []byte{0}
//
// ソフトデリート済みの同一キーが存在する場合は Update で上書きする
func (ui *UniqueIndex) Insert(bp *buffer.BufferPool, encodedPK []byte, columns [][]byte) error {
	btr := btree.NewBTree(ui.MetaPageId)

	// ユニークキーをエンコード
	var encodedUk []byte
	encode.Encode([][]byte{columns[ui.UkIdx]}, &encodedUk)

	// 同一ユニークキー を持つ active なレコードが存在するか確認 (ユニーク制約チェック)
	if err := ui.checkUniqueConstraint(bp, btr, encodedUk); err != nil {
		return err
	}

	// fullKey = concat(encodedUk, encodedPK)
	fullKey := make([]byte, 0, len(encodedUk)+len(encodedPK))
	fullKey = append(fullKey, encodedUk...)
	fullKey = append(fullKey, encodedPK...)

	btrRecord := node.NewRecord([]byte{0}, fullKey, nil)
	err := btr.Insert(bp, btrRecord)
	if err != nil {
		if !errors.Is(err, btree.ErrDuplicateKey) {
			return err
		}

		// 重複キーの場合、既存がソフトデリート済みなら Update で上書き
		existing, findErr := btr.FindByKey(bp, fullKey)
		if findErr != nil {
			return findErr
		}
		if existing.HeaderBytes()[0] != 1 {
			return btree.ErrDuplicateKey
		}

		err = btr.Update(bp, btrRecord)
		if err != nil {
			return err
		}
	}

	return nil
}

// Delete はユニークインデックスから行を物理削除する
//   - encodedPK: エンコード済みプライマリキー
//   - columns: 行の全カラム値
func (ui *UniqueIndex) Delete(bp *buffer.BufferPool, encodedPK []byte, columns [][]byte) error {
	btr := btree.NewBTree(ui.MetaPageId)
	fullKey := ui.getFullKey(encodedPK, columns)
	return btr.Delete(bp, fullKey)
}

// SoftDelete はユニークインデックスから行をソフトデリートする
//   - encodedPK: エンコード済みプライマリキー
//   - columns: 行の全カラム値
func (ui *UniqueIndex) SoftDelete(bp *buffer.BufferPool, encodedPK []byte, columns [][]byte) error {
	btr := btree.NewBTree(ui.MetaPageId)
	fullKey := ui.getFullKey(encodedPK, columns)
	return btr.Update(bp, node.NewRecord([]byte{1}, fullKey, nil))
}

// LeafPageCount は B+Tree のメタページからリーフページ数を取得する
func (ui *UniqueIndex) LeafPageCount(bp *buffer.BufferPool) (uint64, error) {
	btr := btree.NewBTree(ui.MetaPageId)
	return btr.LeafPageCount(bp)
}

// Height は B+Tree のメタページからツリーの高さを取得する
func (ui *UniqueIndex) Height(bp *buffer.BufferPool) (uint64, error) {
	btr := btree.NewBTree(ui.MetaPageId)
	return btr.Height(bp)
}

func (ui *UniqueIndex) getFullKey(encodedPK []byte, columns [][]byte) []byte {
	// ユニークキーをエンコード
	var encodedUk []byte
	encode.Encode([][]byte{columns[ui.UkIdx]}, &encodedUk)

	// fullKey = concat(encodedUk, encodedPK)
	fullKey := make([]byte, 0, len(encodedUk)+len(encodedPK))
	fullKey = append(fullKey, encodedUk...)
	fullKey = append(fullKey, encodedPK...)

	return fullKey
}

// checkUniqueConstraint は encodedUk に対して active なレコードが存在するか確認する
//
// 存在する場合は ErrDuplicateKey を返す
func (ui *UniqueIndex) checkUniqueConstraint(bp *buffer.BufferPool, btr *btree.BTree, encodedUk []byte) error {
	iter, err := btr.Search(bp, btree.SearchModeKey{Key: encodedUk})
	if err != nil {
		return err
	}

	// encodedUk をプレフィックスとして持つレコードを走査する
	for {
		record, ok := iter.Get()
		if !ok {
			break
		}

		// レコードのキーからユニークキー部分を取り出して比較
		// encode.Decode でカラムを分離する
		var keyColumns [][]byte
		encode.Decode(record.KeyBytes(), &keyColumns)
		if len(keyColumns) == 0 {
			break
		}

		// ユニークキーカラムだけ再エンコードして比較
		var existingUk []byte
		encode.Encode(keyColumns[:1], &existingUk)
		if !bytes.Equal(existingUk, encodedUk) {
			// ユニークキーが異なる → これ以上のレコードは一致しない
			break
		}

		// active なレコードが存在する場合はユニーク制約違反
		if record.HeaderBytes()[0] != 1 {
			return btree.ErrDuplicateKey
		}

		// ソフトデリート済みなので次のレコードを確認
		if err := iter.Advance(bp); err != nil {
			return err
		}
	}

	return nil
}
