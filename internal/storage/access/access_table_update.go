package access

import (
	"github.com/ren-yamanashi/minesql/internal/storage/lock"
	"github.com/ren-yamanashi/minesql/internal/storage/undo"
)

// Update はテーブルの行をインプレース更新する
//   - currentRecord: 更新前のレコード (executor が Search で取得したもの)
//   - colNames: 更新するカラム名 (SET 句の対象)
//   - values: 更新後の値
func (t *Table) Update(currentRecord *PrimaryRecord, colNames, values []string, trxId lock.TrxId) error {
	// 更新後のレコードを生成
	newRecord, err := currentRecord.update(trxId, colNames, values)
	if err != nil {
		return err
	}

	// Undo ログを更新
	undoRecord := undo.NewUpdateRecord(t.Table.MetaPageId.FileId, currentRecord.Encode(), newRecord.Encode(), currentRecord.lastTrxId, currentRecord.rollPtr)
	ptr, err := t.undoLog.Append(trxId, undo.Update, undoRecord)
	if err != nil {
		return err
	}
	newRecord.setRollPtr(ptr)

	// レコード更新
	if err := t.primaryIndex.Update(currentRecord, newRecord, trxId); err != nil {
		return err
	}
	return t.updateSecondaryIndexes(currentRecord, colNames, values, trxId)
}

// updateSecondaryIndexes はセカンダリインデックスを更新する
// インデックスを構成するカラムの値が変更される場合のみ、論理削除 + 新規挿入で更新する
func (t *Table) updateSecondaryIndexes(before *PrimaryRecord, updateColNames, updateValues []string, trxId lock.TrxId) error {
	updatedCols := t.buildValMap(updateColNames, updateValues)
	oldValMap := t.buildValMap(before.ColNames, before.Values)

	// 更新後の値マップ (before をベースに更新カラムだけ上書き)
	newValMap := t.buildValMap(before.ColNames, before.Values)
	for name, val := range updatedCols {
		newValMap[name] = val
	}

	pk := t.extractPrimaryKey(before.Values)

	for _, si := range t.secondaryIndexes {
		keyCols, err := fetchIndexKeyCol(t.catalog, si.indexId)
		if err != nil {
			return err
		}

		// このインデックスを構成するカラムが更新対象に含まれるか判定
		if !t.isIndexAffected(keyCols, updatedCols) {
			continue
		}

		// 更新前のセカンダリキーで論理削除
		beforeSkColNames, beforeSkValues := t.extractSecondaryKey(keyCols, oldValMap)
		oldSr, err := t.buildSecondaryRecord(si, beforeSkColNames, beforeSkValues, pk)
		if err != nil {
			return err
		}
		if err := si.SoftDelete(oldSr, trxId); err != nil {
			return err
		}

		// 更新後のセカンダリキーで新規挿入
		afterSkColNames, afterSkValues := t.extractSecondaryKey(keyCols, newValMap)
		record, err := newSecondaryRecord(si.catalog, newSecondaryRecordInput{
			fileId:     si.fileId,
			deleteMark: 0,
			indexName:  si.indexName,
			colNames:   afterSkColNames,
			values:     afterSkValues,
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

// isIndexAffected はインデックスを構成するカラムが更新対象に含まれるか判定する
func (t *Table) isIndexAffected(keyCols map[string]int, updatedCols map[string]string) bool {
	for name := range keyCols {
		if _, ok := updatedCols[name]; ok {
			return true
		}
	}
	return false
}
