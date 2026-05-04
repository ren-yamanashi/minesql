package catalog

import "github.com/ren-yamanashi/minesql/internal/storage/btree"

type IndexIterator struct {
	iterator *btree.Iterator
}

func newIndexIterator(iter *btree.Iterator) *IndexIterator {
	return &IndexIterator{iterator: iter}
}

// Next はインデックスメタデータから次の結果を返す
func (ii *IndexIterator) Next() (IndexRecord, bool, error) {
	record, ok, err := ii.iterator.Next()
	if err != nil {
		return IndexRecord{}, false, err
	}
	if !ok {
		return IndexRecord{}, false, nil
	}
	return decodeIndexRecord(record), true, nil
}
