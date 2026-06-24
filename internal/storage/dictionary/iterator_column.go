package dictionary

import "github.com/ren-yamanashi/minesql/internal/storage/btree"

type ColumnIterator struct {
	iterator *btree.Iterator
}

func NewColumnIterator(iter *btree.Iterator) *ColumnIterator {
	return &ColumnIterator{iterator: iter}
}

// Next はカラムメタデータから次の結果を返す
func (ci *ColumnIterator) Next() (ColumnMetaRecord, bool, error) {
	record, ok, err := ci.iterator.Next()
	if err != nil {
		return ColumnMetaRecord{}, false, err
	}
	if !ok {
		return ColumnMetaRecord{}, false, nil
	}
	decoded, err := DecodeColumnMetaRecord(record)
	if err != nil {
		return ColumnMetaRecord{}, false, err
	}
	return decoded, true, nil
}
