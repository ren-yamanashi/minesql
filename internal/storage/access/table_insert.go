package access

import (
	"github.com/ren-yamanashi/minesql/internal/storage/buffer"
	"github.com/ren-yamanashi/minesql/internal/storage/lock"
	"github.com/ren-yamanashi/minesql/internal/storage/undo"
)

// Insert はテーブルに行を挿入する
func (t *Table) Insert(trx *Transaction, colNames []string, values []string) error {
	trxId := trx.trxId
	mtr := buffer.NewWriteMtr(t.bufferPool, trxId, t.redoLog)
	defer mtr.UnpinAll()

	// FK チェック (親レコードに共有ロックを取得する。挿入キーへの排他ロックより前に行うため順序は親 S → 子 X)
	if err := t.checkForeignKeysForInsert(trxId, colNames, values); err != nil {
		return err
	}

	record, err := NewPrimaryRecord(t.catalog, t.bufferPool, NewPrimaryRecordInput{
		fileId:     t.primaryIndex.fileId(),
		pkCount:    t.primaryIndex.pkCount,
		deleteMark: 0,
		lastTrxId:  trxId,
		colNames:   colNames,
		values:     values,
	})
	if err != nil {
		return err
	}

	// Undo ログを更新
	undoRecord := undo.NewInsertRecord(t.primaryIndex.fileId(), record.Encode())
	ptr, err := t.undoLog.Append(mtr, trxId, undo.RecordTypeInsert, undoRecord)
	if err != nil {
		return err
	}
	record.setRollPtr(ptr)

	// レコード挿入
	if err := t.primaryIndex.insert(mtr, record, trxId); err != nil {
		return err
	}
	if err := t.insertSecondaryIndexes(mtr, record.colNames, record.values, trxId); err != nil {
		return err
	}
	return mtr.Commit()
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
		record, err := t.buildSecondaryRecord(si, skColNames, skValues, pk, trxId)
		if err != nil {
			return err
		}
		if err := si.insert(mtr, record, trxId); err != nil {
			return err
		}
	}
	return nil
}
