package access

import (
	"github.com/ren-yamanashi/minesql/internal/storage/lock"
	"github.com/ren-yamanashi/minesql/internal/storage/undo"
)

// Insert はテーブルに行を挿入する
func (t *Table) Insert(colNames []string, values []string, trxId lock.TrxId) error {
	record, err := newPrimaryRecord(t.catalog, newPrimaryRecordInput{
		fileId:     t.primaryIndex.fileId(),
		pkCount:    t.primaryIndex.pkCount,
		deleteMark: 0,
		colNames:   colNames,
		values:     values,
	})
	if err != nil {
		return err
	}

	// Undo ログを更新
	undoRecord := undo.NewInsertRecord(t.primaryIndex.fileId(), record.encode())
	ptr, err := t.undoLog.Append(trxId, undo.RecordTypeInsert, undoRecord)
	if err != nil {
		return err
	}
	record.setRollPtr(ptr)

	// レコード挿入
	if err := t.primaryIndex.insert(record, trxId); err != nil {
		return err
	}
	return t.insertSecondaryIndexes(record.ColNames, record.Values, trxId)
}

// insertSecondaryIndexes は全セカンダリインデックスにレコードを挿入する
func (t *Table) insertSecondaryIndexes(colNames, values []string, trxId lock.TrxId) error {
	valMap := t.buildValMap(colNames, values)
	pk := t.extractPrimaryKey(values)

	for _, si := range t.secondaryIndexes {
		keyCols, err := fetchIndexKeyCol(t.catalog, si.indexId)
		if err != nil {
			return err
		}
		skColNames, skValues := t.extractSecondaryKey(keyCols, valMap)
		record, err := t.buildSecondaryRecord(si, skColNames, skValues, pk)
		if err != nil {
			return err
		}
		if err := si.insert(record, pk, trxId); err != nil {
			return err
		}
	}
	return nil
}
