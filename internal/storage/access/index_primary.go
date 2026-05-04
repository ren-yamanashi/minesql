package access

import (
	"errors"

	"github.com/ren-yamanashi/minesql/internal/storage/btree"
	"github.com/ren-yamanashi/minesql/internal/storage/buffer"
	"github.com/ren-yamanashi/minesql/internal/storage/catalog"
	"github.com/ren-yamanashi/minesql/internal/storage/page"
)

// PrimaryIndex はプライマリインデックスへのアクセスを提供する
type PrimaryIndex struct {
	catalog *catalog.Catalog
	tree    *btree.Btree // プライマリインデックスの B+Tree
	pkCount int          // プライマリキーのカラム数
}

// NewPrimaryIndex は既存のプライマリインデックスを開く
func NewPrimaryIndex(ct *catalog.Catalog, bp *buffer.BufferPool, metaPageId page.PageId, pkCount int) *PrimaryIndex {
	tree := btree.NewBtree(bp, metaPageId)
	return &PrimaryIndex{
		catalog: ct,
		tree:    tree,
		pkCount: pkCount,
	}
}

// CreatePrimaryIndex は空のプライマリインデックスを作成する
func CreatePrimaryIndex(ct *catalog.Catalog, bp *buffer.BufferPool, fileId page.FileId, pkCount int) (*PrimaryIndex, error) {
	tree, err := btree.CreateBtree(bp, fileId)
	if err != nil {
		return nil, err
	}
	return &PrimaryIndex{
		catalog: ct,
		tree:    tree,
		pkCount: pkCount,
	}, nil
}

// Search は指定した検索モードでテーブルを検索し、イテレータを返す
func (pi *PrimaryIndex) Search(mode SearchMode) (*PrimaryIterator, error) {
	iter, err := pi.tree.Search(mode.encode())
	if err != nil {
		return nil, err
	}
	return newPrimaryIterator(iter, pi.catalog, pi.tree.MetaPageId.FileId), nil
}

// Insert は行を挿入する
// (論理削除済みの同一キーが存在する場合は上書きする)
func (pi *PrimaryIndex) Insert(colNames, value []string) error {
	// TODO: Undo ログの記録
	record, err := newPrimaryRecord(pi.catalog, newPrimaryRecordInput{
		fileId:     pi.tree.MetaPageId.FileId,
		pkCount:    pi.pkCount,
		deleteMark: 0,
		colNames:   colNames,
		values:     value,
	})
	if err != nil {
		return err
	}
	return pi.insert(record)
}

// SoftDelete は行を論理削除する
func (pi *PrimaryIndex) SoftDelete(record *PrimaryRecord) error {
	// TODO: Undo ログの記録
	colNames := make([]string, len(record.ColNames))
	values := make([]string, len(record.Values))
	copy(colNames, record.ColNames)
	copy(values, record.Values)
	deleted := &PrimaryRecord{
		pkCount:    record.pkCount,
		deleteMark: 1,
		ColNames:   colNames,
		Values:     values,
	}
	return pi.softDelete(deleted)
}

// UpdateInplace は行を更新する
func (pi *PrimaryIndex) UpdateInplace(currentRecord *PrimaryRecord, colNames, value []string) error {
	// TODO: Undo ログの記録
	newRecord, err := currentRecord.update(colNames, value)
	if err != nil {
		return err
	}
	return pi.updateInplace(newRecord)
}

// LeafPageCount はリーフページ数を取得する
func (pi *PrimaryIndex) LeafPageCount() (uint64, error) {
	return pi.tree.LeafPageCount()
}

// Height はツリーの高さを取得する
func (pi *PrimaryIndex) Height() (uint64, error) {
	return pi.tree.Height()
}

// insert は Undo ログを記録せずテーブルに行を挿入する
func (pi *PrimaryIndex) insert(pr *PrimaryRecord) error {
	// TODO: ロック処理
	record := pr.encode()
	err := pi.tree.Insert(record)
	if err == nil {
		return nil
	}
	if !errors.Is(err, btree.ErrDuplicateKey) {
		return err
	}

	// 重複キーエラーの場合、既存のレコードが論理削除済みか確認
	existing, _, err := pi.tree.FindByKey(record.Key())
	if err != nil {
		return err
	}

	deleteMark := existing.Header()[0]
	if deleteMark != 1 {
		return btree.ErrDuplicateKey
	}

	// 論理削除済みの場合は上書き
	return pi.tree.Update(record)
}

// softDelete は Undo ログを記録せず行を論理削除する
func (pi *PrimaryIndex) softDelete(pr *PrimaryRecord) error {
	// TODO: ロック処理
	return pi.tree.Update(pr.encode())
}

// delete は Undo ログを記録せず行を削除する
func (pi *PrimaryIndex) delete(pr *PrimaryRecord) error {
	// TODO: ロック処理
	return pi.tree.Delete(pr.encode().Key())
}

// updateInplace は Undo ログを記録せず行を更新する
func (pi *PrimaryIndex) updateInplace(newRecord *PrimaryRecord) error {
	// TODO: ロック処理
	return pi.tree.Update(newRecord.encode())
}
