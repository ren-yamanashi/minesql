package catalog

import "github.com/ren-yamanashi/minesql/internal/storage/btree"

type IndexKeyColumnIterator struct {
	iterator *btree.Iterator
}

func NewIndexKeyColumnIterator(iter *btree.Iterator) *IndexKeyColumnIterator {
	return &IndexKeyColumnIterator{iterator: iter}
}

// Close はイテレータが保持しているバッファページの参照を解放する
func (iki *IndexKeyColumnIterator) Close() {
	iki.iterator.Close()
}

// Next はインデックスキーカラムメタデータから次の結果を返す
func (iki *IndexKeyColumnIterator) Next() (IndexKeyColumnRecord, bool, error) {
	record, ok, err := iki.iterator.Next()
	if err != nil {
		return IndexKeyColumnRecord{}, false, err
	}
	if !ok {
		return IndexKeyColumnRecord{}, false, nil
	}
	return DecodeIndexKeyColumnRecord(record), true, nil
}
