package access

// SoftDelete はテーブルの行を論理削除する
func (t *Table) SoftDelete(record *PrimaryRecord) error {
	if err := t.primaryIndex.SoftDelete(record); err != nil {
		return err
	}
	return t.deleteSecondaryIndexes(record)
}

// deleteSecondaryIndexes は全セカンダリインデックスのレコードを論理削除する
func (t *Table) deleteSecondaryIndexes(record *PrimaryRecord) error {
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
		if err := si.SoftDelete(sr); err != nil {
			return err
		}
	}
	return nil
}
