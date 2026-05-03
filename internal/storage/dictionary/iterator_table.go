package dictionary

import "github.com/ren-yamanashi/minesql/internal/storage/btree"

type TableIterator struct {
	iterator *btree.Iterator
}

func newTableIterator(iter *btree.Iterator) *TableIterator {
	return &TableIterator{iterator: iter}
}

// Next はテーブルメタデータから次の結果を返す
func (imi *TableIterator) Next() (tableRecord, bool, error) {
	record, ok, err := imi.iterator.Next()
	if err != nil {
		return tableRecord{}, false, err
	}
	if !ok {
		return tableRecord{}, false, nil
	}
	return decodeTableRecord(record), true, nil
}
