package catalog

import "github.com/ren-yamanashi/minesql/internal/storage/btree"

type UserIterator struct {
	iterator *btree.Iterator
}

func NewUserIterator(iter *btree.Iterator) *UserIterator {
	return &UserIterator{iterator: iter}
}

// Close はイテレータが保持しているバッファページの参照を解放する
func (ui *UserIterator) Close() {
	ui.iterator.Close()
}

// Next はユーザーメタデータから次の結果を返す
func (ui *UserIterator) Next() (UserRecord, bool, error) {
	record, ok, err := ui.iterator.Next()
	if err != nil {
		return UserRecord{}, false, err
	}
	if !ok {
		return UserRecord{}, false, nil
	}
	return DecodeUserRecord(record), true, nil
}
