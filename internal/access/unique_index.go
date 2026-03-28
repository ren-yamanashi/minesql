package access

import (
	"bytes"
	"errors"
	"minesql/internal/storage/btree"
	"minesql/internal/storage/bufferpool"
	"minesql/internal/storage/memcomparable"
	"minesql/internal/storage/page"
)

// UniqueIndexAccessMethod はユニークインデックスへのアクセスを提供する
type UniqueIndexAccessMethod struct {
	Name            string      // インデックス名
	ColName         string      // インデックスを構成するカラム名
	MetaPageId      page.PageId // インデックスの内容が入っている B+Tree のメタページの ID
	SecondaryKeyIdx uint16      // セカンダリキーに含めるカラムのインデックス (0 始まりの列番号)
	PrimaryKeyCount uint8       // PK のカラム数 (key からセカンダリキーと PK を分離するために必要)
}

func NewUniqueIndexAccessMethod(name string, colName string, metaPageId page.PageId, secondaryKeyIdx uint16) *UniqueIndexAccessMethod {
	return &UniqueIndexAccessMethod{
		Name:            name,
		ColName:         colName,
		MetaPageId:      metaPageId,
		SecondaryKeyIdx: secondaryKeyIdx,
	}
}

// Search は指定した検索モードでインデックスを検索し、SecondaryIndexIterator を返す
func (ui *UniqueIndexAccessMethod) Search(bp *bufferpool.BufferPool, table *TableAccessMethod, mode RecordSearchMode) (*SecondaryIndexIterator, error) {
	indexBTree := btree.NewBPlusTree(ui.MetaPageId)
	indexIter, err := indexBTree.Search(bp, mode.encode())
	if err != nil {
		return nil, err
	}
	tableBTree := btree.NewBPlusTree(table.MetaPageId)
	return newSecondaryIndexIterator(indexIter, tableBTree, bp, ui.PrimaryKeyCount), nil
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
// Key = concat(encodedSecondaryKey, encodedPK), NonKey = nil, Header = []byte{0}
//
// ソフトデリート済みの同一キーが存在する場合は Update で上書きする
func (ui *UniqueIndexAccessMethod) Insert(bp *bufferpool.BufferPool, encodedPK []byte, columns [][]byte) error {
	btr := btree.NewBPlusTree(ui.MetaPageId)

	// セカンダリキーをエンコード
	var encodedSecondaryKey []byte
	memcomparable.Encode([][]byte{columns[ui.SecondaryKeyIdx]}, &encodedSecondaryKey)

	// ユニーク制約チェック: active なレコード (同一セカンダリキー) が存在するか確認
	if err := ui.checkUniqueConstraint(bp, btr, encodedSecondaryKey); err != nil {
		return err
	}

	// fullKey = concat(encodedSecondaryKey, encodedPK)
	fullKey := make([]byte, 0, len(encodedSecondaryKey)+len(encodedPK))
	fullKey = append(fullKey, encodedSecondaryKey...)
	fullKey = append(fullKey, encodedPK...)

	btrRecord := btree.NewRecord([]byte{0}, fullKey, nil)
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

// Delete はユニークインデックスからソフトデリートする
//
// encodedPK: エンコード済みプライマリキー
//
// columns: 行の全カラム値
func (ui *UniqueIndexAccessMethod) Delete(bp *bufferpool.BufferPool, encodedPK []byte, columns [][]byte) error {
	btr := btree.NewBPlusTree(ui.MetaPageId)

	// セカンダリキーをエンコード
	var encodedSecondaryKey []byte
	memcomparable.Encode([][]byte{columns[ui.SecondaryKeyIdx]}, &encodedSecondaryKey)

	// fullKey = concat(encodedSecondaryKey, encodedPK)
	fullKey := make([]byte, 0, len(encodedSecondaryKey)+len(encodedPK))
	fullKey = append(fullKey, encodedSecondaryKey...)
	fullKey = append(fullKey, encodedPK...)

	// DeleteMark を 1 にしてインプレース更新
	btrRecord := btree.NewRecord([]byte{1}, fullKey, nil)
	return btr.Update(bp, btrRecord)
}

// LeafPageCount は B+Tree のメタページからリーフページ数を取得する
func (ui *UniqueIndexAccessMethod) LeafPageCount(bp *bufferpool.BufferPool) (uint64, error) {
	btr := btree.NewBPlusTree(ui.MetaPageId)
	return btr.LeafPageCount(bp)
}

// Height は B+Tree のメタページからツリーの高さを取得する
func (ui *UniqueIndexAccessMethod) Height(bp *bufferpool.BufferPool) (uint64, error) {
	btr := btree.NewBPlusTree(ui.MetaPageId)
	return btr.Height(bp)
}

// checkUniqueConstraint は encodedSecondaryKey に対して active なレコードが存在するか確認する
//
// 存在する場合は ErrDuplicateKey を返す
func (ui *UniqueIndexAccessMethod) checkUniqueConstraint(bp *bufferpool.BufferPool, btr *btree.BPlusTree, encodedSecondaryKey []byte) error {
	iter, err := btr.Search(bp, btree.SearchModeKey{Key: encodedSecondaryKey})
	if err != nil {
		return err
	}

	// encodedSecondaryKey をプレフィックスとして持つレコードを走査する
	for {
		record, ok := iter.Get()
		if !ok {
			break
		}

		// レコードのキーからセカンダリキー部分を取り出して比較
		// memcomparable.Decode でカラムを分離する
		var keyColumns [][]byte
		memcomparable.Decode(record.KeyBytes(), &keyColumns)
		if len(keyColumns) == 0 {
			break
		}

		// セカンダリキーカラムだけ再エンコードして比較
		var existingSecondaryKey []byte
		memcomparable.Encode(keyColumns[:1], &existingSecondaryKey)
		if !bytes.Equal(existingSecondaryKey, encodedSecondaryKey) {
			// セカンダリキーが異なる → これ以上のレコードは一致しない
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
