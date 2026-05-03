package dictionary

import "github.com/ren-yamanashi/minesql/internal/storage/btree"

type ConstraintIterator struct {
	iterator *btree.Iterator
}

func newConstraintIterator(iter *btree.Iterator) *ConstraintIterator {
	return &ConstraintIterator{iterator: iter}
}

// Next は制約メタデータから次の結果を返す
func (imi *ConstraintIterator) Next() (constraintRecord, bool, error) {
	record, ok, err := imi.iterator.Next()
	if err != nil {
		return constraintRecord{}, false, err
	}
	if !ok {
		return constraintRecord{}, false, nil
	}
	return decodeConstraintRecord(record), true, nil
}
