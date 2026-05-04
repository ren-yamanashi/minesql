package catalog

import "github.com/ren-yamanashi/minesql/internal/storage/btree"

type ConstraintIterator struct {
	iterator *btree.Iterator
}

func newConstraintIterator(iter *btree.Iterator) *ConstraintIterator {
	return &ConstraintIterator{iterator: iter}
}

// Next は制約メタデータから次の結果を返す
func (ci *ConstraintIterator) Next() (ConstraintRecord, bool, error) {
	record, ok, err := ci.iterator.Next()
	if err != nil {
		return ConstraintRecord{}, false, err
	}
	if !ok {
		return ConstraintRecord{}, false, nil
	}
	return decodeConstraintRecord(record), true, nil
}
