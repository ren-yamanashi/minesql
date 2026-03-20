package access

import (
	"minesql/internal/storage/btree"
	"minesql/internal/storage/bufferpool"
	"minesql/internal/storage/memcomparable"
)

type ClusteredIndexIterator struct {
	iterator *btree.Iterator
	bp       *bufferpool.BufferPool
}

func newClusteredIndexIterator(iterator *btree.Iterator, bp *bufferpool.BufferPool) *ClusteredIndexIterator {
	return &ClusteredIndexIterator{
		iterator: iterator,
		bp:       bp,
	}
}

// Next はデコード済みの次のレコードを返す
//
// 戻り値: レコード (プライマリキー + 値), データがあるかどうか, エラー
func (ri *ClusteredIndexIterator) Next() ([][]byte, bool, error) {
	pair, ok, err := ri.iterator.Next(ri.bp)
	if !ok {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, err
	}

	// レコード (プライマリキー + 値) をデコード
	var record [][]byte
	memcomparable.Decode(pair.Key, &record)
	memcomparable.Decode(pair.Value, &record)

	return record, true, nil
}
