package catalog

import "github.com/ren-yamanashi/minesql/internal/storage/btree"

type IndexKeyColIterator struct {
	iterator *btree.Iterator
}

func newIndexKeyColIterator(iter *btree.Iterator) *IndexKeyColIterator {
	return &IndexKeyColIterator{iterator: iter}
}

// Next はインデックスキーカラムメタデータから次の結果を返す
func (iki *IndexKeyColIterator) Next() (IndexKeyColRecord, bool, error) {
	record, ok, err := iki.iterator.Next()
	if err != nil {
		return IndexKeyColRecord{}, false, err
	}
	if !ok {
		return IndexKeyColRecord{}, false, nil
	}
	return decodeIndexKeyColRecord(record), true, nil
}
