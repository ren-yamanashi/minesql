package access

import (
	"bytes"
	"errors"

	"github.com/ren-yamanashi/minesql/internal/storage/btree"
	"github.com/ren-yamanashi/minesql/internal/storage/btree/node"
	"github.com/ren-yamanashi/minesql/internal/storage/buffer"
	"github.com/ren-yamanashi/minesql/internal/storage/encode"
	"github.com/ren-yamanashi/minesql/internal/storage/page"
)

// SecondaryIndex はセカンダリインデックスへのアクセスを提供する
//
// Unique が true の場合はユニーク制約を適用する
type SecondaryIndex struct {
	Name       string      // インデックス名
	ColName    string      // インデックスを構成するカラム名
	MetaPageId page.PageId // インデックスの内容が入っている B+Tree のメタページの ID
	ColIdx     uint16      // インデックスカラムの位置 (0 始まりの列番号)
	PkCount    uint8       // PK のカラム数 (key からセカンダリキーと PK を分離するために必要)
	Unique     bool        // ユニーク制約の有無
}

func NewSecondaryIndex(name string, colName string, metaPageId page.PageId, colIdx uint16, pkCount uint8, unique bool) *SecondaryIndex {
	return &SecondaryIndex{
		Name:       name,
		ColName:    colName,
		MetaPageId: metaPageId,
		ColIdx:     colIdx,
		PkCount:    pkCount,
		Unique:     unique,
	}
}

// Search は指定した検索モードでインデックスを検索し、SecondaryIndexIterator を返す
func (si *SecondaryIndex) Search(bp *buffer.BufferPool, table *Table, mode RecordSearchMode) (*SecondaryIndexIterator, error) {
	indexBTree := btree.NewBTree(si.MetaPageId)
	indexIter, err := indexBTree.Search(bp, mode.encode())
	if err != nil {
		return nil, err
	}
	tableBTree := btree.NewBTree(table.MetaPageId)
	return newSecondaryIndexIterator(indexIter, tableBTree, bp, si.PkCount), nil
}

// Create は空のセカンダリインデックスを新規作成する
func (si *SecondaryIndex) Create(bp *buffer.BufferPool) error {
	btr, err := btree.CreateBTree(bp, si.MetaPageId)
	if err != nil {
		return err
	}
	si.MetaPageId = btr.MetaPageId
	return nil
}

// Insert はセカンダリインデックスに行を挿入する
//
// Key = concat(encodedSecondaryKey, encodedPK), NonKey = nil, Header = []byte{0}
//
// ソフトデリート済みの同一キーが存在する場合は Update で上書きする
func (si *SecondaryIndex) Insert(bp *buffer.BufferPool, encodedPK []byte, columns [][]byte) error {
	btr := btree.NewBTree(si.MetaPageId)

	// セカンダリキーをエンコード
	var encodedSecKey []byte
	encode.Encode([][]byte{columns[si.ColIdx]}, &encodedSecKey)

	// ユニーク制約チェック (Unique の場合のみ)
	if si.Unique {
		if err := si.checkUniqueConstraint(bp, btr, encodedSecKey); err != nil {
			return err
		}
	}

	// fullKey = concat(encodedSecKey, encodedPK)
	fullKey := make([]byte, 0, len(encodedSecKey)+len(encodedPK))
	fullKey = append(fullKey, encodedSecKey...)
	fullKey = append(fullKey, encodedPK...)

	btrRecord := node.NewRecord([]byte{0}, fullKey, nil)
	err := btr.Insert(bp, btrRecord)
	if err != nil {
		if !errors.Is(err, btree.ErrDuplicateKey) {
			return err
		}

		// 重複キーの場合、既存がソフトデリート済みなら Update で上書き
		existing, _, findErr := btr.FindByKey(bp, fullKey)
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

// Delete はセカンダリインデックスから行を物理削除する
//   - encodedPK: エンコード済みプライマリキー
//   - columns: 行の全カラム値
func (si *SecondaryIndex) Delete(bp *buffer.BufferPool, encodedPK []byte, columns [][]byte) error {
	btr := btree.NewBTree(si.MetaPageId)
	fullKey := si.getFullKey(encodedPK, columns)
	return btr.Delete(bp, fullKey)
}

// SoftDelete はセカンダリインデックスから行をソフトデリートする
//   - encodedPK: エンコード済みプライマリキー
//   - columns: 行の全カラム値
func (si *SecondaryIndex) SoftDelete(bp *buffer.BufferPool, encodedPK []byte, columns [][]byte) error {
	btr := btree.NewBTree(si.MetaPageId)
	fullKey := si.getFullKey(encodedPK, columns)
	return btr.Update(bp, node.NewRecord([]byte{1}, fullKey, nil))
}

// LeafPageCount は B+Tree のメタページからリーフページ数を取得する
func (si *SecondaryIndex) LeafPageCount(bp *buffer.BufferPool) (uint64, error) {
	btr := btree.NewBTree(si.MetaPageId)
	return btr.LeafPageCount(bp)
}

// Height は B+Tree のメタページからツリーの高さを取得する
func (si *SecondaryIndex) Height(bp *buffer.BufferPool) (uint64, error) {
	btr := btree.NewBTree(si.MetaPageId)
	return btr.Height(bp)
}

func (si *SecondaryIndex) getFullKey(encodedPK []byte, columns [][]byte) []byte {
	// セカンダリキーをエンコード
	var encodedSecKey []byte
	encode.Encode([][]byte{columns[si.ColIdx]}, &encodedSecKey)

	// fullKey = concat(encodedSecKey, encodedPK)
	fullKey := make([]byte, 0, len(encodedSecKey)+len(encodedPK))
	fullKey = append(fullKey, encodedSecKey...)
	fullKey = append(fullKey, encodedPK...)

	return fullKey
}

// checkUniqueConstraint は encodedSecKey に対して active なレコードが存在するか確認する
//
// 存在する場合は ErrDuplicateKey を返す
func (si *SecondaryIndex) checkUniqueConstraint(bp *buffer.BufferPool, btr *btree.BTree, encodedSecKey []byte) error {
	iter, err := btr.Search(bp, btree.SearchModeKey{Key: encodedSecKey})
	if err != nil {
		return err
	}

	// encodedSecKey をプレフィックスとして持つレコードを走査する
	for {
		record, ok := iter.Get()
		if !ok {
			break
		}

		// レコードのキーからセカンダリキー部分を取り出して比較
		var keyColumns [][]byte
		encode.Decode(record.KeyBytes(), &keyColumns)
		if len(keyColumns) == 0 {
			break
		}

		// セカンダリキーカラムだけ再エンコードして比較
		var existingSecKey []byte
		encode.Encode(keyColumns[:1], &existingSecKey)
		if !bytes.Equal(existingSecKey, encodedSecKey) {
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
