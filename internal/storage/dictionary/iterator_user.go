package dictionary

import "github.com/ren-yamanashi/minesql/internal/storage/btree"

type UserIterator struct {
	iterator *btree.Iterator
}

func NewUserIterator(iter *btree.Iterator) *UserIterator {
	return &UserIterator{iterator: iter}
}

// Next はユーザーメタデータから次の結果を返す
func (ui *UserIterator) Next() (UserMetaRecord, bool, error) {
	record, ok, err := ui.iterator.Next()
	if err != nil {
		return UserMetaRecord{}, false, err
	}
	if !ok {
		return UserMetaRecord{}, false, nil
	}
	decoded, err := DecodeUserMetaRecord(record)
	if err != nil {
		return UserMetaRecord{}, false, err
	}
	return decoded, true, nil
}
