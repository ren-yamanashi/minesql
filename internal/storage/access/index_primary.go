package access

import (
	"errors"

	"github.com/ren-yamanashi/minesql/internal/storage/btree"
	"github.com/ren-yamanashi/minesql/internal/storage/buffer"
	"github.com/ren-yamanashi/minesql/internal/storage/dictionary"
	"github.com/ren-yamanashi/minesql/internal/storage/lock"
	"github.com/ren-yamanashi/minesql/internal/storage/page"
	"github.com/ren-yamanashi/minesql/internal/storage/undo"
)

type primaryIndex struct {
	catalog    *dictionary.Catalog
	bufferPool *buffer.Pool
	tree       *btree.Tree // プライマリインデックスの B+Tree
	pkCount    int         // プライマリキーのカラム数
	lock       *lock.Manager
	undoLog    *undo.Manager
}

// newPrimaryIndex は既存のプライマリインデックスを開く
func newPrimaryIndex(
	ct *dictionary.Catalog,
	bp *buffer.Pool,
	metaPageId page.Id,
	pkCount int,
	lock *lock.Manager,
	undoLog *undo.Manager,
) *primaryIndex {
	tree := btree.NewTree(bp, metaPageId)
	return &primaryIndex{
		catalog:    ct,
		bufferPool: bp,
		tree:       tree,
		pkCount:    pkCount,
		lock:       lock,
		undoLog:    undoLog,
	}
}

// createPrimaryIndex は空のプライマリインデックスを作成する
func createPrimaryIndex(
	ct *dictionary.Catalog,
	bp *buffer.Pool,
	fileId page.FileId,
	pkCount int,
	lock *lock.Manager,
	undoLog *undo.Manager,
) (*primaryIndex, error) {
	tree, err := btree.CreateTree(bp, fileId)
	if err != nil {
		return nil, err
	}
	return &primaryIndex{
		catalog:    ct,
		bufferPool: bp,
		tree:       tree,
		pkCount:    pkCount,
		lock:       lock,
		undoLog:    undoLog,
	}, nil
}

// search は指定した検索モードでテーブルを検索し、イテレータを返す
//   - readView が非 nil の場合は MVCC の可視性判定 + Undo 遡及を行う
func (pi *primaryIndex) search(mtr *buffer.Mtr, mode SearchMode, readView *readView) (*PrimaryIndexIterator, error) {
	iter, err := pi.tree.Search(mtr, mode.Encode())
	if err != nil {
		return nil, err
	}
	return NewPrimaryIndexIterator(iter, pi.catalog, pi.bufferPool, pi.tree.MetaPageId().FileId(), readView, pi.undoLog, mtr), nil
}

// insert は行を挿入する
// (論理削除済みの同一キーが存在する場合は上書きする)
func (pi *primaryIndex) insert(mtr *buffer.Mtr, record *PrimaryRecord, trxId lock.TrxId) error {
	encodedRecord := record.Encode()

	// 排他ロックを取得
	rowKey := lock.RowKey{MetaPageId: pi.tree.MetaPageId(), Key: encodedRecord.Key()}
	if err := pi.lock.Lock(trxId, rowKey, lock.Exclusive); err != nil {
		return err
	}

	// 挿入
	err := pi.tree.Insert(mtr, encodedRecord)
	// 重複キーエラーの場合、既存のレコードが論理削除済みか確認
	if errors.Is(err, btree.ErrDuplicateKey) {
		existing, _, findErr := pi.tree.FindByKey(mtr, encodedRecord.Key())
		if findErr != nil {
			return findErr
		}
		deleteMark := existing.Header()[0]
		// 論理削除済みでない場合はエラー
		if deleteMark != 1 {
			return btree.ErrDuplicateKey
		}
		// 論理削除済みの場合は上書き
		return pi.tree.Update(mtr, encodedRecord)
	}
	return err
}

// delete は 行を物理削除する
func (pi *primaryIndex) delete(mtr *buffer.Mtr, record *PrimaryRecord, trxId lock.TrxId) error {
	encodedRecord := record.Encode()

	// 排他ロックを取得
	rowKey := lock.RowKey{MetaPageId: pi.tree.MetaPageId(), Key: encodedRecord.Key()}
	if err := pi.lock.Lock(trxId, rowKey, lock.Exclusive); err != nil {
		return err
	}

	// 物理削除
	return pi.tree.Delete(mtr, encodedRecord.Key())
}

// softDelete は行を論理削除する
func (pi *primaryIndex) softDelete(mtr *buffer.Mtr, record *PrimaryRecord, trxId lock.TrxId) error {
	encodedRecord := record.Encode()

	// 排他ロックを取得
	rowKey := lock.RowKey{MetaPageId: pi.tree.MetaPageId(), Key: encodedRecord.Key()}
	if err := pi.lock.Lock(trxId, rowKey, lock.Exclusive); err != nil {
		return err
	}

	// 論理削除
	// deleteMark を 1 にしたレコードで上書き
	deleted, err := NewPrimaryRecord(pi.catalog, pi.bufferPool, NewPrimaryRecordInput{
		fileId:     pi.tree.MetaPageId().FileId(),
		pkCount:    record.pkCount,
		deleteMark: 1,
		lastTrxId:  trxId,
		rollPtr:    record.rollPtr,
		colNames:   record.colNames,
		values:     record.values,
	})
	if err != nil {
		return err
	}
	return pi.tree.Update(mtr, deleted.Encode())
}

// update は行を更新する
func (pi *primaryIndex) update(mtr *buffer.Mtr, newRecord *PrimaryRecord, trxId lock.TrxId) error {
	encodedRecord := newRecord.Encode()

	// 排他ロックを取得
	rowKey := lock.RowKey{MetaPageId: pi.tree.MetaPageId(), Key: encodedRecord.Key()}
	if err := pi.lock.Lock(trxId, rowKey, lock.Exclusive); err != nil {
		return err
	}

	// 更新
	return pi.tree.Update(mtr, encodedRecord)
}

// fileId はテーブルの fileId を返す
func (pi *primaryIndex) fileId() page.FileId {
	return pi.tree.MetaPageId().FileId()
}

// leafPageCount はリーフページ数を取得する
func (pi *primaryIndex) leafPageCount() (uint64, error) {
	return pi.tree.LeafPageCount()
}

// height はツリーの高さを取得する
func (pi *primaryIndex) height() (uint64, error) {
	return pi.tree.Height()
}
