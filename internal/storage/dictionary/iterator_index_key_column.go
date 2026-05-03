package dictionary

import "github.com/ren-yamanashi/minesql/internal/storage/btree"

type IndexKeyColIterator struct {
	iterator *btree.Iterator
}

func newIndexKeyColIterator(iter *btree.Iterator) *IndexKeyColIterator {
	return &IndexKeyColIterator{iterator: iter}
}

// Next はインデックスキーカラムメタデータから次の結果を返す
func (imi *IndexKeyColIterator) Next() (indexKeyColRecord, bool, error) {
	record, ok, err := imi.iterator.Next()
	if err != nil {
		return indexKeyColRecord{}, false, err
	}
	if !ok {
		return indexKeyColRecord{}, false, nil
	}
	return decodeIndexKeyColRecord(record), true, nil
}
