package access

import (
	"bytes"
	"errors"
	"slices"

	"github.com/ren-yamanashi/minesql/internal/storage/btree"
	"github.com/ren-yamanashi/minesql/internal/storage/buffer"
	"github.com/ren-yamanashi/minesql/internal/storage/page"
	"github.com/ren-yamanashi/minesql/internal/storage/undo"
)

var errUnknownUndoRecordType = errors.New("unknown undo record type")

// RollbackToSavepoint は savepoint 以降の Undo レコードを逆順に適用して文の効果だけを取り消す
//   - savepoint は Transaction.Savepoint で取得した値
//   - 全件成功後に savepoint 以降の Undo エントリを破棄する
//   - ロックは解放しない (トランザクションは継続する)
//   - エラーを返した場合、 savepoint 以降の Undo は保持される。 呼び出し側はトランザクション全体 Rollback を呼ぶこと
func (t *TrxManager) RollbackToSavepoint(trx *Transaction, savepoint int) error {
	records := t.undoLog.RecordsFrom(trx.trxId, savepoint)
	for _, r := range slices.Backward(records) {
		mtr := buffer.NewWriteMtr(t.bufferPool, trx.trxId, t.redoLog)
		rollbackErr := t.rollbackRecord(mtr, r)
		if commitErr := mtr.Commit(); commitErr != nil && rollbackErr == nil {
			rollbackErr = commitErr
		}
		if rollbackErr != nil {
			return rollbackErr
		}
	}
	t.undoLog.DiscardFrom(trx.trxId, savepoint)
	return nil
}

// rollbackRecord は 1 つの Undo レコードに対応するロールバック操作を実行する
//   - mtr のライフサイクル (Commit / UnpinAll) は呼び出し側が管理する
//   - 通常運用時は書き込み Mtr を、Recovery 中は読み取り Mtr を渡すことで Redo 記録の有無を切り替える
func (t *TrxManager) rollbackRecord(mtr *buffer.Mtr, record undo.Record) error {
	fileId := record.TableFileId()
	piRecord, err := fetchPrimaryIndexRecord(t.catalog, t.bufferPool, fileId)
	if err != nil {
		return err
	}
	primaryTree := btree.NewTree(t.bufferPool, piRecord.MetaPageId())

	switch r := record.(type) {
	case undo.InsertRecord:
		return t.rollbackInsert(mtr, primaryTree, r, fileId)
	case undo.DeleteRecord:
		return t.rollbackDelete(mtr, primaryTree, r, fileId)
	case undo.UpdateRecord:
		return t.rollbackUpdate(mtr, primaryTree, r, fileId)
	default:
		return errUnknownUndoRecordType
	}
}

// rollbackInsert は Insert を取り消す (Primary, Secondary の物理削除)
//   - 部分適用済みの状態への再実行を許容するため、対象キーが既に存在しない (ErrKeyNotFound) 場合は成功として扱う
func (t *TrxManager) rollbackInsert(mtr *buffer.Mtr, primaryTree *btree.Tree, record undo.InsertRecord, fileId page.FileId) error {
	if err := primaryTree.Delete(mtr, record.Record().Key()); err != nil && !errors.Is(err, btree.ErrKeyNotFound) {
		return err
	}
	primaryRecord, err := DecodePrimaryRecord(record.Record(), t.catalog, t.bufferPool, fileId)
	if err != nil {
		return err
	}
	return t.forEachSecondaryTree(fileId, func(tree *btree.Tree, keyCols map[string]int) error {
		key := primaryRecord.SecondaryKey(keyCols)
		if err := tree.Delete(mtr, key); err != nil && !errors.Is(err, btree.ErrKeyNotFound) {
			return err
		}
		return nil
	})
}

// rollbackDelete は SoftDelete を取り消す (Primary, Secondary の DeleteMark 復元)
func (t *TrxManager) rollbackDelete(mtr *buffer.Mtr, primaryTree *btree.Tree, record undo.DeleteRecord, fileId page.FileId) error {
	// Undo ログには削除前のレコード (DeleteMark=0) が保存されているので、削除前のレコードで上書き
	if err := primaryTree.Update(mtr, record.Record()); err != nil {
		return err
	}
	primaryRecord, err := DecodePrimaryRecord(record.Record(), t.catalog, t.bufferPool, fileId)
	if err != nil {
		return err
	}
	return t.forEachSecondaryTree(fileId, func(tree *btree.Tree, keyCols map[string]int) error {
		key := primaryRecord.SecondaryKey(keyCols)
		restored := btree.NewRecord(make([]byte, secondaryHeaderSize), key, nil) // header: 5 byte zero-fill (deleteMark=0, lastTrxId=0), key: sk+pk, nonKey: nil
		return tree.Update(mtr, restored)
	})
}

// rollbackUpdate は Update を取り消す (Primary を旧レコードで上書き + Secondary 復元)
//   - 部分適用済みの状態への再実行を許容するため、Secondary の新キー物理削除は ErrKeyNotFound を成功として扱う
//   - Primary / Secondary の旧値上書きは同値再適用で自然に冪等
func (t *TrxManager) rollbackUpdate(mtr *buffer.Mtr, primaryTree *btree.Tree, record undo.UpdateRecord, fileId page.FileId) error {
	if err := primaryTree.Update(mtr, record.PrevRecord()); err != nil {
		return err
	}
	prevPrimaryRecord, err := DecodePrimaryRecord(record.PrevRecord(), t.catalog, t.bufferPool, fileId)
	if err != nil {
		return err
	}
	newPrimaryRecord, err := DecodePrimaryRecord(record.NewRecord(), t.catalog, t.bufferPool, fileId)
	if err != nil {
		return err
	}
	return t.forEachSecondaryTree(fileId, func(tree *btree.Tree, keyCols map[string]int) error {
		oldKey := prevPrimaryRecord.SecondaryKey(keyCols)
		newKey := newPrimaryRecord.SecondaryKey(keyCols)
		// SK が変わってない場合はスキップ
		if bytes.Equal(oldKey, newKey) {
			return nil
		}
		// 更新後の SK を物理削除し、更新前の SK を復元 (論理削除を元に戻す)
		if err := tree.Delete(mtr, newKey); err != nil && !errors.Is(err, btree.ErrKeyNotFound) {
			return err
		}
		restored := btree.NewRecord(make([]byte, secondaryHeaderSize), oldKey, nil) // header: 5 byte zero-fill (deleteMark=0, lastTrxId=0), key: sk+pk, nonKey: nil
		return tree.Update(mtr, restored)
	})
}

// forEachSecondaryTree は指定テーブルの全セカンダリインデックスに対してコールバックを実行する
func (t *TrxManager) forEachSecondaryTree(
	fileId page.FileId,
	op func(tree *btree.Tree, keyCols map[string]int) error,
) error {
	records, err := fetchSecondaryIndexRecords(t.catalog, t.bufferPool, fileId)
	if err != nil {
		return err
	}
	for _, record := range records {
		keyCols, err := fetchIndexKeyColumn(t.catalog, t.bufferPool, record.IndexId())
		if err != nil {
			return err
		}
		tree := btree.NewTree(t.bufferPool, record.MetaPageId())
		if err := op(tree, keyCols); err != nil {
			return err
		}
	}
	return nil
}
