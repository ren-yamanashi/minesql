package access

import (
	"bytes"
	"errors"

	"github.com/ren-yamanashi/minesql/internal/storage/btree"
	"github.com/ren-yamanashi/minesql/internal/storage/buffer"
	"github.com/ren-yamanashi/minesql/internal/storage/page"
)

// SecondaryIndex はセカンダリインデックスへのアクセスを提供する
type SecondaryIndex struct {
	tree    *btree.Btree // セカンダリインデックスの B+Tree
	skCount int          // セカンダリキーのカラム数
	unique  bool         // ユニーク制約の有無
}

// NewSecondaryIndex は既存のセカンダリインデックスを開く
func NewSecondaryIndex(bp *buffer.BufferPool, metaPageId page.PageId, skCount int, unique bool) *SecondaryIndex {
	tree := btree.NewBtree(bp, metaPageId)
	return &SecondaryIndex{
		tree:    tree,
		skCount: skCount,
		unique:  unique,
	}
}

// CreateSecondaryIndex は空のセカンダリインデックスを作成する
//   - fileId: セカンダリインデックスを格納するファイルの ID
func CreateSecondaryIndex(bp *buffer.BufferPool, fileId page.FileId, skCount int, unique bool) (*SecondaryIndex, error) {
	tree, err := btree.CreateBtree(bp, fileId)
	if err != nil {
		return nil, err
	}
	return &SecondaryIndex{
		tree:    tree,
		skCount: skCount,
		unique:  unique,
	}, nil
}

// Insert は行を挿入する
//   - data: SK + PK
//   - unique index の場合かつセカンダリキーの重複があるとエラー
//   - 論理削除済みの同一キー (SK + PK) が存在する場合は上書きする
func (si *SecondaryIndex) Insert(data [][]byte) error {
	record := newSecondaryRecord(si.skCount, 0, data)
	if si.unique {
		if err := si.checkUnique(record); err != nil {
			return err
		}
	}
	return si.insert(record)
}

// Delete は行を物理削除する
//   - data: SK + PK
func (si *SecondaryIndex) Delete(data [][]byte) error {
	// TODO: ロック処理
	key := newSecondaryRecord(si.skCount, 0, data).encode().Key()
	return si.tree.Delete(key)
}

// SoftDelete は行を論理削除する
//   - data: SK + PK
func (si *SecondaryIndex) SoftDelete(data [][]byte) error {
	// TODO: ロック処理
	record := newSecondaryRecord(si.skCount, 1, data).encode()
	existing, _, err := si.tree.FindByKey(record.Key())
	if err != nil {
		return err
	}

	deleteMark := existing.Header()[0]
	if deleteMark == 1 {
		return ErrAlreadyDeleted
	}

	return si.tree.Update(record)
}

// LeafPageCount はリーフページ数を取得する
func (si *SecondaryIndex) LeafPageCount() (uint64, error) {
	return si.tree.LeafPageCount()
}

// Height はツリーの高さを取得する
func (si *SecondaryIndex) Height() (uint64, error) {
	return si.tree.Height()
}

// insert は行を挿入する
func (si *SecondaryIndex) insert(sr *SecondaryRecord) error {
	// TODO: ロック処理
	record := sr.encode()
	err := si.tree.Insert(record)
	if err == nil {
		return nil
	}
	if !errors.Is(err, btree.ErrDuplicateKey) {
		return err
	}

	// 重複キーエラーの場合、既存のレコードが論理削除済みか確認
	existing, _, err := si.tree.FindByKey(record.Key())
	if err != nil {
		return err
	}

	deleteMark := existing.Header()[0]
	if deleteMark != 1 {
		return btree.ErrDuplicateKey
	}

	// 論理削除済みの場合は上書き
	return si.tree.Update(record)
}

// checkUnique は record のセカンダリキー に対して active なレコードが存在するか確認する
//   - return: 存在する場合は ErrDuplicateKey
func (si *SecondaryIndex) checkUnique(sr *SecondaryRecord) error {
	encodedSk := sr.encodedSecondaryKey()
	// セカンダリインデックスのキーは SK+PK の構成であり、SK のみで SearchModeKey を使うと SK 以上の最初のキーの位置に着地する
	iter, err := si.tree.Search(btree.SearchModeKey{Key: encodedSk})
	if err != nil {
		return err
	}

	for {
		existing, ok, err := iter.Get()
		if err != nil {
			return err
		}
		if !ok {
			return nil
		}

		// キー同士の比較
		if len(existing.Key()) < len(encodedSk) {
			return nil
		}
		existingSk := existing.Key()[:len(encodedSk)]
		if !bytes.Equal(existingSk, encodedSk) {
			return nil
		}

		deleteMark := existing.Header()[0]
		// 論理削除済みでない場合は重複
		if deleteMark != 1 {
			return btree.ErrDuplicateKey
		}
		// 論理削除済みの場合は次のレコードへ
		if err := iter.Advance(); err != nil {
			return err
		}
	}
}
