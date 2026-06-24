package dictionary

import "github.com/ren-yamanashi/minesql/internal/storage/btree"

type ConstraintIterator struct {
	iterator *btree.Iterator
}

func NewConstraintIterator(iter *btree.Iterator) *ConstraintIterator {
	return &ConstraintIterator{iterator: iter}
}

// Next は制約メタデータから次の結果を返す
func (ci *ConstraintIterator) Next() (ConstraintMetaRecord, bool, error) {
	record, ok, err := ci.iterator.Next()
	if err != nil {
		return ConstraintMetaRecord{}, false, err
	}
	if !ok {
		return ConstraintMetaRecord{}, false, nil
	}
	decoded, err := DecodeConstraintMetaRecord(record)
	if err != nil {
		return ConstraintMetaRecord{}, false, err
	}
	return decoded, true, nil
}
