package access

// Insert はテーブルに行を挿入する
func (t *Table) Insert(colNames []string, values []string) error {
	if err := t.primaryIndex.Insert(colNames, values); err != nil {
		return err
	}
	return t.insertSecondaryIndexes(colNames, values)
}

// insertSecondaryIndexes は全セカンダリインデックスにレコードを挿入する
func (t *Table) insertSecondaryIndexes(colNames, values []string) error {
	valMap := t.buildValMap(colNames, values)
	pk := t.extractPrimaryKey(values)

	for _, si := range t.secondaryIndexes {
		keyCols, err := fetchIndexKeyCol(t.catalog, si.indexId)
		if err != nil {
			return err
		}
		skColNames, skValues := t.extractSecondaryKey(keyCols, valMap)
		if err := si.Insert(skColNames, skValues, pk); err != nil {
			return err
		}
	}
	return nil
}
