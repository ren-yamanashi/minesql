package access

import (
	"github.com/ren-yamanashi/minesql/internal/storage/lock"
	"github.com/ren-yamanashi/minesql/internal/storage/undo"
)

// SoftDelete はテーブルの行を論理削除する
func (t *Table) SoftDelete(record *PrimaryRecord, trxId lock.TrxId) error {
	// Undo ログを更新
	undoRecord := undo.NewDeleteRecord(t.Table.MetaPageId.FileId, record.Encode(), record.lastTrxId, record.rollPtr)
	ptr, err := t.undoLog.Append(trxId, undo.RecordTypeDelete, undoRecord)
	if err != nil {
		return err
	}
	record.setRollPtr(ptr)

	// レコード削除
	if err := t.primaryIndex.SoftDelete(record, trxId); err != nil {
		return err
	}
	return t.softDeleteSecondaryIndexes(record, trxId)
}

// Delete はテーブルの行を物理削除する
// (物理削除は DML 操作では行われないので、Undo ログの作成はしない)
func (t *Table) Delete(record *PrimaryRecord, trxId lock.TrxId) error {
	if err := t.primaryIndex.Delete(record, trxId); err != nil {
		return err
	}
	return t.deleteSecondaryIndexes(record, trxId)
}

// softDeleteSecondaryIndexes は全セカンダリインデックスのレコードを論理削除する
func (t *Table) softDeleteSecondaryIndexes(record *PrimaryRecord, trxId lock.TrxId) error {
	return t.forEachSecondaryRecord(record, func(si *SecondaryIndex, sr *SecondaryRecord) error {
		return si.SoftDelete(sr, trxId)
	})
}

// deleteSecondaryIndexes は全セカンダリインデックスのレコードを物理削除する
func (t *Table) deleteSecondaryIndexes(record *PrimaryRecord, trxId lock.TrxId) error {
	return t.forEachSecondaryRecord(record, func(si *SecondaryIndex, sr *SecondaryRecord) error {
		return si.Delete(sr, trxId)
	})
}

// forEachSecondaryRecord は PrimaryRecord から各セカンダリインデックス用のレコードを構築し、op を適用する
func (t *Table) forEachSecondaryRecord(record *PrimaryRecord, op func(*SecondaryIndex, *SecondaryRecord) error) error {
	valMap := t.buildValMap(record.ColNames, record.Values)
	pk := t.extractPrimaryKey(record.Values)

	for _, si := range t.secondaryIndexes {
		keyCols, err := fetchIndexKeyCol(t.catalog, si.indexId)
		if err != nil {
			return err
		}
		skColNames, skValues := t.extractSecondaryKey(keyCols, valMap)
		sr, err := t.buildSecondaryRecord(si, skColNames, skValues, pk)
		if err != nil {
			return err
		}
		if err := op(si, sr); err != nil {
			return err
		}
	}
	return nil
}
