package access

import (
	"errors"

	"github.com/ren-yamanashi/minesql/internal/storage/btree"
	"github.com/ren-yamanashi/minesql/internal/storage/buffer"
	"github.com/ren-yamanashi/minesql/internal/storage/catalog"
	"github.com/ren-yamanashi/minesql/internal/storage/lock"
	"github.com/ren-yamanashi/minesql/internal/storage/page"
)

// primaryIndex はプライマリインデックスへのアクセスを提供する
type primaryIndex struct {
	catalog *catalog.Catalog
	tree    *btree.Btree // プライマリインデックスの B+Tree
	pkCount int          // プライマリキーのカラム数
	lock    *lock.Manager
}

// newPrimaryIndex は既存のプライマリインデックスを開く
func newPrimaryIndex(ct *catalog.Catalog, bp *buffer.BufferPool, metaPageId page.PageId, pkCount int, lock *lock.Manager) *primaryIndex {
	tree := btree.NewBtree(bp, metaPageId)
	return &primaryIndex{
		catalog: ct,
		tree:    tree,
		pkCount: pkCount,
		lock:    lock,
	}
}

// createPrimaryIndex は空のプライマリインデックスを作成する
func createPrimaryIndex(ct *catalog.Catalog, bp *buffer.BufferPool, fileId page.FileId, pkCount int, lock *lock.Manager) (*primaryIndex, error) {
	tree, err := btree.CreateBtree(bp, fileId)
	if err != nil {
		return nil, err
	}
	return &primaryIndex{
		catalog: ct,
		tree:    tree,
		pkCount: pkCount,
		lock:    lock,
	}, nil
}

// search は指定した検索モードでテーブルを検索し、イテレータを返す
func (pi *primaryIndex) search(mode SearchMode) (*primaryIterator, error) {
	iter, err := pi.tree.Search(mode.encode())
	if err != nil {
		return nil, err
	}
	return newPrimaryIterator(iter, pi.catalog, pi.tree.MetaPageId.FileId), nil
}

// insert は行を挿入する
// (論理削除済みの同一キーが存在する場合は上書きする)
func (pi *primaryIndex) insert(record *primaryRecord, trxId lock.TrxId) error {
	// 挿入
	encodedRecord := record.encode()
	err := pi.tree.Insert(encodedRecord)

	// 重複キーエラーの場合、既存のレコードが論理削除済みか確認
	if errors.Is(err, btree.ErrDuplicateKey) {
		existing, _, findErr := pi.tree.FindByKey(encodedRecord.Key())
		if findErr != nil {
			return findErr
		}
		deleteMark := existing.Header()[0]
		// 論理削除済みでない場合はエラー
		if deleteMark != 1 {
			return btree.ErrDuplicateKey
		}
		// 論理削除済みの場合は上書き
		if updateErr := pi.tree.Update(encodedRecord); updateErr != nil {
			return updateErr
		}
	} else if err != nil {
		return err
	}

	// 排他ロックを取得
	_, pos, err := pi.tree.FindByKey(encodedRecord.Key())
	if err != nil {
		return err
	}
	return pi.lock.Lock(trxId, pos, lock.Exclusive)
}

// delete は 行を物理削除する
func (pi *primaryIndex) delete(record *primaryRecord, trxId lock.TrxId) error {
	// 排他ロックを取得
	encodedRecord := record.encode()
	_, pos, err := pi.tree.FindByKey(encodedRecord.Key())
	if err != nil {
		return err
	}
	if err := pi.lock.Lock(trxId, pos, lock.Exclusive); err != nil {
		return err
	}

	// 物理削除
	return pi.tree.Delete(encodedRecord.Key())
}

// softDelete は行を論理削除する
func (pi *primaryIndex) softDelete(record *primaryRecord, trxId lock.TrxId) error {
	// 排他ロックを取得
	encodedRecord := record.encode()
	_, pos, err := pi.tree.FindByKey(encodedRecord.Key())
	if err != nil {
		return err
	}
	if err := pi.lock.Lock(trxId, pos, lock.Exclusive); err != nil {
		return err
	}

	// 論理削除
	// deleteMark を 1 にしたレコードで上書き
	deleted, err := newPrimaryRecord(pi.catalog, newPrimaryRecordInput{
		fileId:     pi.tree.MetaPageId.FileId,
		pkCount:    record.pkCount,
		deleteMark: 1,
		lastTrxId:  trxId,
		rollPtr:    record.rollPtr,
		colNames:   record.ColNames,
		values:     record.Values,
	})
	if err != nil {
		return err
	}
	return pi.tree.Update(deleted.encode())
}

// update は行を更新する
func (pi *primaryIndex) update(currentRecord *primaryRecord, newRecord *primaryRecord, trxId lock.TrxId) error {
	// 排他ロックを取得
	encodedRecord := newRecord.encode()
	_, pos, err := pi.tree.FindByKey(encodedRecord.Key())
	if err != nil {
		return err
	}
	if err := pi.lock.Lock(trxId, pos, lock.Exclusive); err != nil {
		return err
	}

	// 更新
	return pi.tree.Update(encodedRecord)
}

// fileId はテーブルの fileId を返す
func (pi *primaryIndex) fileId() page.FileId {
	return pi.tree.MetaPageId.FileId
}

// leafPageCount はリーフページ数を取得する
func (pi *primaryIndex) leafPageCount() (uint64, error) {
	return pi.tree.LeafPageCount()
}

// height はツリーの高さを取得する
func (pi *primaryIndex) height() (uint64, error) {
	return pi.tree.Height()
}
