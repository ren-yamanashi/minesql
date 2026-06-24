package dictionary

import "github.com/ren-yamanashi/minesql/internal/storage/btree"

type IndexKeyColumnIterator struct {
	iterator *btree.Iterator
}

func NewIndexKeyColumnIterator(iter *btree.Iterator) *IndexKeyColumnIterator {
	return &IndexKeyColumnIterator{iterator: iter}
}

// Next はインデックスキーカラムメタデータから次の結果を返す
func (iki *IndexKeyColumnIterator) Next() (IndexKeyColumnMetaRecord, bool, error) {
	record, ok, err := iki.iterator.Next()
	if err != nil {
		return IndexKeyColumnMetaRecord{}, false, err
	}
	if !ok {
		return IndexKeyColumnMetaRecord{}, false, nil
	}
	decoded, err := DecodeIndexKeyColumnMetaRecord(record)
	if err != nil {
		return IndexKeyColumnMetaRecord{}, false, err
	}
	return decoded, true, nil
}
