package catalog

import "github.com/ren-yamanashi/minesql/internal/storage/btree"

type tableIterator struct {
	iterator *btree.Iterator
}

func newTableIterator(iter *btree.Iterator) *tableIterator {
	return &tableIterator{iterator: iter}
}

// Next はテーブルメタデータから次の結果を返す
func (ti *tableIterator) Next() (TableRecord, bool, error) {
	record, ok, err := ti.iterator.Next()
	if err != nil {
		return TableRecord{}, false, err
	}
	if !ok {
		return TableRecord{}, false, nil
	}
	return decodeTableRecord(record), true, nil
}
