package access

import (
	"github.com/ren-yamanashi/minesql/internal/storage/buffer"
	"github.com/ren-yamanashi/minesql/internal/storage/lock"
	"github.com/ren-yamanashi/minesql/internal/storage/undo"
)

// Update はテーブルの行を更新する
//   - currentRecord は SearchForUpdate (Current Read) で取得した排他ロック済みの最新バージョンを渡すこと
//   - PK カラムが更新対象に含まれない場合はインプレース更新を行う
//   - PK カラムが更新対象に含まれる場合は論理削除 + 新規挿入で実現する
func (t *Table) Update(trx *Transaction, currentRecord *PrimaryRecord, colNames, values []string) error {
	trxId := trx.trxId
	newRecord, err := currentRecord.update(trxId, colNames, values)
	if err != nil {
		return err
	}

	if t.isPrimaryKeyChanged(currentRecord, newRecord) {
		// PK が変わる場合は論理削除 + 新規挿入 (それぞれの操作内で mtr 境界が記録される)
		if err := t.SoftDelete(trx, currentRecord); err != nil {
			return err
		}
		return t.Insert(trx, newRecord.colNames, newRecord.values)
	}

	if _, err := t.redoLog.AppendMtrStart(trxId); err != nil {
		return err
	}
	defer func() { _, _ = t.redoLog.AppendMtrEnd(trxId) }()

	// PK が変わらない場合はインプレース更新
	mtr := buffer.NewMtr(t.bufferPool)
	defer mtr.UnpinAll()

	// FK チェック
	if err := t.checkForeignKeysForUpdate(currentRecord, newRecord); err != nil {
		return err
	}

	// Undo ログを記録
	undoRecord := undo.NewUpdateRecord(
		t.primaryIndex.fileId(),
		currentRecord.Encode(),
		newRecord.Encode(),
		currentRecord.lastTrxId,
		currentRecord.rollPtr,
	)
	ptr, err := t.undoLog.Append(mtr, trxId, undo.RecordTypeUpdate, undoRecord)
	if err != nil {
		return err
	}
	newRecord.setRollPtr(ptr)

	// レコード更新
	if err := t.primaryIndex.update(mtr, newRecord, trxId); err != nil {
		return err
	}
	return t.updateSecondaryIndexes(mtr, currentRecord, colNames, values, trxId)
}

// updateSecondaryIndexes はセカンダリインデックスを更新する
// インデックスを構成するカラムの値が変更される場合のみ、論理削除 + 新規挿入で更新する
func (t *Table) updateSecondaryIndexes(
	mtr *buffer.Mtr,
	before *PrimaryRecord,
	updateColNames, updateValues []string,
	trxId lock.TrxId,
) error {
	updatedCols := t.buildValMap(updateColNames, updateValues)
	oldValMap := t.buildValMap(before.colNames, before.values)

	// 更新後の値マップ (before をベースに更新カラムだけ上書き)
	newValMap := t.buildValMap(before.colNames, before.values)
	for name, val := range updatedCols {
		newValMap[name] = val
	}

	pk := t.extractPrimaryKey(before.values)

	for _, si := range t.secondaryIndexes {
		keyCols, err := fetchIndexKeyColumn(t.catalog, t.bufferPool, si.indexId)
		if err != nil {
			return err
		}

		// このインデックスを構成するカラムが更新対象に含まれるか判定
		if !t.isIndexAffected(keyCols, updatedCols) {
			continue
		}

		// 更新前のセカンダリキーで論理削除
		beforeSkColNames, beforeSkValues := t.extractSecondaryKey(keyCols, oldValMap)
		oldSr, err := t.buildSecondaryRecord(si, beforeSkColNames, beforeSkValues, pk, trxId)
		if err != nil {
			return err
		}
		if err := si.softDelete(mtr, oldSr, trxId); err != nil {
			return err
		}

		// 更新後のセカンダリキーで新規挿入
		afterSkColNames, afterSkValues := t.extractSecondaryKey(keyCols, newValMap)
		record, err := t.buildSecondaryRecord(si, afterSkColNames, afterSkValues, pk, trxId)
		if err != nil {
			return err
		}
		if err := si.insert(mtr, record, trxId); err != nil {
			return err
		}
	}
	return nil
}

// isPrimaryKeyChanged は更新前後でプライマリキーの値が変わるかどうかを判定する
func (t *Table) isPrimaryKeyChanged(before, after *PrimaryRecord) bool {
	pkCount := t.primaryIndex.pkCount
	for i := range pkCount {
		if before.values[i] != after.values[i] {
			return true
		}
	}
	return false
}

// isIndexAffected はインデックスを構成するカラムが更新対象に含まれるか判定する
func (t *Table) isIndexAffected(keyCols map[string]int, updatedCols map[string]string) bool {
	for name := range keyCols {
		if _, ok := updatedCols[name]; ok {
			return true
		}
	}
	return false
}
