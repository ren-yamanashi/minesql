package catalog

import "github.com/ren-yamanashi/minesql/internal/storage/btree"

type IndexIterator struct {
	iterator *btree.Iterator
}

func NewIndexIterator(iter *btree.Iterator) *IndexIterator {
	return &IndexIterator{iterator: iter}
}

// Close はイテレータが保持しているバッファページの参照を解放する
func (ii *IndexIterator) Close() {
	ii.iterator.Close()
}

// Next はインデックスメタデータから次の結果を返す
func (ii *IndexIterator) Next() (IndexRecord, bool, error) {
	record, ok, err := ii.iterator.Next()
	if err != nil {
		return IndexRecord{}, false, err
	}
	if !ok {
		return IndexRecord{}, false, nil
	}
	return DecodeIndexRecord(record), true, nil
}
