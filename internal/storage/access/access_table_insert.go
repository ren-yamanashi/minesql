package access

import (
	"github.com/ren-yamanashi/minesql/internal/storage/lock"
	"github.com/ren-yamanashi/minesql/internal/storage/undo"
)

// Insert はテーブルに行を挿入する
func (t *Table) Insert(colNames []string, values []string, trxId lock.TrxId) error {
	record, err := newPrimaryRecord(t.catalog, newPrimaryRecordInput{
		fileId:     t.Table.MetaPageId.FileId,
		pkCount:    t.primaryIndex.pkCount,
		deleteMark: 0,
		colNames:   colNames,
		values:     values,
	})
	if err != nil {
		return err
	}

	// Undo ログを更新
	undoRecord := undo.NewInsertRecord(t.Table.MetaPageId.FileId, record.Encode())
	ptr, err := t.undoLog.Append(trxId, undo.Insert, undoRecord)
	if err != nil {
		return err
	}
	record.setRollPtr(ptr)

	// レコード挿入
	if err := t.primaryIndex.Insert(record, trxId); err != nil {
		return err
	}
	return t.insertSecondaryIndexes(colNames, values, trxId)
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
		record, err := newSecondaryRecord(si.catalog, newSecondaryRecordInput{
			fileId:     si.fileId,
			deleteMark: 0,
			indexName:  si.indexName,
			colNames:   skColNames,
			values:     skValues,
			pk:         pk,
		})
		if err != nil {
			return err
		}
		if err := si.Insert(record, pk, trxId); err != nil {
			return err
		}
	}
	return nil
}
