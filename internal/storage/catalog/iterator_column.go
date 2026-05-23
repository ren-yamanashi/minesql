package catalog

import "github.com/ren-yamanashi/minesql/internal/storage/btree"

type columnIterator struct {
	iterator *btree.Iterator
}

func newColumnIterator(iter *btree.Iterator) *columnIterator {
	return &columnIterator{iterator: iter}
}

// Next はカラムメタデータから次の結果を返す
func (ci *columnIterator) Next() (ColumnRecord, bool, error) {
	record, ok, err := ci.iterator.Next()
	if err != nil {
		return ColumnRecord{}, false, err
	}
	if !ok {
		return ColumnRecord{}, false, nil
	}
	return decodeColumnRecord(record), true, nil
}
