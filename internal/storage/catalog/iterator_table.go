package catalog

import "github.com/ren-yamanashi/minesql/internal/storage/btree"

type TableIterator struct {
	iterator *btree.Iterator
}

func newTableIterator(iter *btree.Iterator) *TableIterator {
	return &TableIterator{iterator: iter}
}

// Next はテーブルメタデータから次の結果を返す
func (imi *TableIterator) Next() (TableRecord, bool, error) {
	record, ok, err := imi.iterator.Next()
	if err != nil {
		return TableRecord{}, false, err
	}
	if !ok {
		return TableRecord{}, false, nil
	}
	return decodeTableRecord(record), true, nil
}
