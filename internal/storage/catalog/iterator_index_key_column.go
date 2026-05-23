package catalog

import "github.com/ren-yamanashi/minesql/internal/storage/btree"

type indexKeyColumnIterator struct {
	iterator *btree.Iterator
}

func newIndexKeyColumnIterator(iter *btree.Iterator) *indexKeyColumnIterator {
	return &indexKeyColumnIterator{iterator: iter}
}

// Next はインデックスキーカラムメタデータから次の結果を返す
func (iki *indexKeyColumnIterator) Next() (IndexKeyColumnRecord, bool, error) {
	record, ok, err := iki.iterator.Next()
	if err != nil {
		return IndexKeyColumnRecord{}, false, err
	}
	if !ok {
		return IndexKeyColumnRecord{}, false, nil
	}
	return decodeIndexKeyColumnRecord(record), true, nil
}
