package catalog

import "github.com/ren-yamanashi/minesql/internal/storage/btree"

type IndexIterator struct {
	iterator *btree.Iterator
}

func newIndexIterator(iter *btree.Iterator) *IndexIterator {
	return &IndexIterator{iterator: iter}
}

// Next はインデックスメタデータから次の結果を返す
func (imi *IndexIterator) Next() (indexRecord, bool, error) {
	record, ok, err := imi.iterator.Next()
	if err != nil {
		return indexRecord{}, false, err
	}
	if !ok {
		return indexRecord{}, false, nil
	}
	return decodeIndexRecord(record), true, nil
}
