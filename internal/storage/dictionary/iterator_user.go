package dictionary

import "github.com/ren-yamanashi/minesql/internal/storage/btree"

type UserIterator struct {
	iterator *btree.Iterator
}

func newUserIterator(iter *btree.Iterator) *UserIterator {
	return &UserIterator{iterator: iter}
}

// Next はユーザーメタデータから次の結果を返す
func (umi *UserIterator) Next() (userRecord, bool, error) {
	record, ok, err := umi.iterator.Next()
	if err != nil {
		return userRecord{}, false, err
	}
	if !ok {
		return userRecord{}, false, nil
	}
	return decodeUserRecord(record), true, nil
}
