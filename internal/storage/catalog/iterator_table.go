package catalog

import "github.com/ren-yamanashi/minesql/internal/storage/btree"

type TableIterator struct {
	iterator *btree.Iterator
}

func NewTableIterator(iter *btree.Iterator) *TableIterator {
	return &TableIterator{iterator: iter}
}

// Close はイテレータが保持しているバッファページの参照を解放する
func (ti *TableIterator) Close() {
	ti.iterator.Close()
}

// Next はテーブルメタデータから次の結果を返す
func (ti *TableIterator) Next() (TableRecord, bool, error) {
	record, ok, err := ti.iterator.Next()
	if err != nil {
		return TableRecord{}, false, err
	}
	if !ok {
		return TableRecord{}, false, nil
	}
	return decodeTableRecord(record), true, nil
}
