package access

import (
	"bytes"
	"errors"

	"github.com/ren-yamanashi/minesql/internal/storage/btree"
	"github.com/ren-yamanashi/minesql/internal/storage/buffer"
	"github.com/ren-yamanashi/minesql/internal/storage/page"
	"github.com/ren-yamanashi/minesql/internal/storage/undo"
)

var errUnknownUndoRecordType = errors.New("unknown undo record type")

// rollbackRecord は 1 つの Undo レコードに対応するロールバック操作を実行する
func (t *TrxManager) rollbackRecord(record undo.Record) error {
	mtr := buffer.NewMtr(t.bufferPool)
	defer mtr.UnpinAll()

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
func (t *TrxManager) rollbackInsert(mtr *buffer.Mtr, primaryTree *btree.Tree, record undo.InsertRecord, fileId page.FileId) error {
	if err := primaryTree.Delete(mtr, record.Record().Key()); err != nil {
		return err
	}
	primaryRecord, err := DecodePrimaryRecord(record.Record(), t.catalog, t.bufferPool, fileId)
	if err != nil {
		return err
	}
	return t.forEachSecondaryTree(fileId, func(tree *btree.Tree, keyCols map[string]int) error {
		key := primaryRecord.SecondaryKey(keyCols)
		return tree.Delete(mtr, key)
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
		if err := tree.Delete(mtr, newKey); err != nil {
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
