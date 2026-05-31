package access

import (
	"github.com/ren-yamanashi/minesql/internal/storage/buffer"
	"github.com/ren-yamanashi/minesql/internal/storage/lock"
	"github.com/ren-yamanashi/minesql/internal/storage/undo"
)

// Insert はテーブルに行を挿入する
func (t *Table) Insert(colNames []string, values []string, trxId lock.TrxId) error {
	if _, err := t.redoLog.AppendMtrStart(trxId); err != nil {
		return err
	}
	defer func() { _, _ = t.redoLog.AppendMtrEnd(trxId) }()

	mtr := buffer.NewMtr(t.bufferPool)
	defer mtr.UnpinAll()

	// FK チェック
	if err := t.checkForeignKeysForInsert(colNames, values); err != nil {
		return err
	}

	record, err := NewPrimaryRecord(t.catalog, t.bufferPool, NewPrimaryRecordInput{
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
	undoRecord := undo.NewInsertRecord(t.primaryIndex.fileId(), record.Encode())
	ptr, err := t.undoLog.Append(trxId, undo.RecordTypeInsert, undoRecord)
	if err != nil {
		return err
	}
	record.setRollPtr(ptr)

	// レコード挿入
	if err := t.primaryIndex.insert(mtr, record, trxId); err != nil {
		return err
	}
	return t.insertSecondaryIndexes(mtr, record.colNames, record.values, trxId)
}

// insertSecondaryIndexes は全セカンダリインデックスにレコードを挿入する
func (t *Table) insertSecondaryIndexes(mtr *buffer.Mtr, colNames, values []string, trxId lock.TrxId) error {
	valMap := t.buildValMap(colNames, values)
	pk := t.extractPrimaryKey(values)

	for _, si := range t.secondaryIndexes {
		keyCols, err := fetchIndexKeyColumn(t.catalog, t.bufferPool, si.indexId)
		if err != nil {
			return err
		}
		skColNames, skValues := t.extractSecondaryKey(keyCols, valMap)
		record, err := t.buildSecondaryRecord(si, skColNames, skValues, pk)
		if err != nil {
			return err
		}
		if err := si.insert(mtr, record, trxId); err != nil {
			return err
		}
	}
	return nil
}
