package dictionary

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
func (ti *TableIterator) Next() (TableMetaRecord, bool, error) {
	record, ok, err := ti.iterator.Next()
	if err != nil {
		return TableMetaRecord{}, false, err
	}
	if !ok {
		return TableMetaRecord{}, false, nil
	}
	decoded, err := DecodeTableMetaRecord(record)
	if err != nil {
		return TableMetaRecord{}, false, err
	}
	return decoded, true, nil
}
