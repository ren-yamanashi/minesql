package catalog

import "github.com/ren-yamanashi/minesql/internal/storage/btree"

type userIterator struct {
	iterator *btree.Iterator
}

func newUserIterator(iter *btree.Iterator) *userIterator {
	return &userIterator{iterator: iter}
}

// Next はユーザーメタデータから次の結果を返す
func (umi *userIterator) Next() (UserRecord, bool, error) {
	record, ok, err := umi.iterator.Next()
	if err != nil {
		return UserRecord{}, false, err
	}
	if !ok {
		return UserRecord{}, false, nil
	}
	return decodeUserRecord(record), true, nil
}
