package dictionary

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
func (ii *IndexIterator) Next() (IndexMetaRecord, bool, error) {
	record, ok, err := ii.iterator.Next()
	if err != nil {
		return IndexMetaRecord{}, false, err
	}
	if !ok {
		return IndexMetaRecord{}, false, nil
	}
	decoded, err := DecodeIndexMetaRecord(record)
	if err != nil {
		return IndexMetaRecord{}, false, err
	}
	return decoded, true, nil
}
