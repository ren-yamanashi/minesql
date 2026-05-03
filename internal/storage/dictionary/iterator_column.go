package dictionary

import "github.com/ren-yamanashi/minesql/internal/storage/btree"

type ColumnIterator struct {
	iterator *btree.Iterator
}

func newColumnIterator(iter *btree.Iterator) *ColumnIterator {
	return &ColumnIterator{iterator: iter}
}

// Next はカラムメタデータから次の結果を返す
func (imi *ColumnIterator) Next() (columnRecord, bool, error) {
	record, ok, err := imi.iterator.Next()
	if err != nil {
		return columnRecord{}, false, err
	}
	if !ok {
		return columnRecord{}, false, nil
	}
	return decodeColumnRecord(record), true, nil
}
