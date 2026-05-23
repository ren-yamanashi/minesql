package catalog

import "github.com/ren-yamanashi/minesql/internal/storage/btree"

type indexKeyColIterator struct {
	iterator *btree.Iterator
}

func newIndexKeyColIterator(iter *btree.Iterator) *indexKeyColIterator {
	return &indexKeyColIterator{iterator: iter}
}

// Next はインデックスキーカラムメタデータから次の結果を返す
func (iki *indexKeyColIterator) Next() (IndexKeyColRecord, bool, error) {
	record, ok, err := iki.iterator.Next()
	if err != nil {
		return IndexKeyColRecord{}, false, err
	}
	if !ok {
		return IndexKeyColRecord{}, false, nil
	}
	return decodeIndexKeyColRecord(record), true, nil
}
