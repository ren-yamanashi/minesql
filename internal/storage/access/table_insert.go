package access

// Insert はテーブルに行を挿入する
func (t *Table) Insert(colNames []string, values []string) error {
	err := t.primaryIndex.Insert(colNames, values)
	if err != nil {
		return err
	}
	keyColNames, sk, pk := t.extractIndexRecord(colNames, values)
	return t.insertIndex(keyColNames, sk, pk)
}

func (t *Table) insertIndex(colNames, values, pk []string) error {
	sis, err := t.fetchSecondaryIndexes()
	if err != nil {
		return err
	}
	for _, index := range sis {
		err := index.Insert(colNames, values, pk)
		if err != nil {
			return err
		}
	}
	return nil
}

func (t *Table) extractIndexRecord(colNames, values []string) (keyColNames, sk, pk []string) {
}
