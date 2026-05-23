package catalog

import "github.com/ren-yamanashi/minesql/internal/storage/btree"

type indexIterator struct {
	iterator *btree.Iterator
}

func newIndexIterator(iter *btree.Iterator) *indexIterator {
	return &indexIterator{iterator: iter}
}

// Next はインデックスメタデータから次の結果を返す
func (ii *indexIterator) Next() (IndexRecord, bool, error) {
	record, ok, err := ii.iterator.Next()
	if err != nil {
		return IndexRecord{}, false, err
	}
	if !ok {
		return IndexRecord{}, false, nil
	}
	return decodeIndexRecord(record), true, nil
}
