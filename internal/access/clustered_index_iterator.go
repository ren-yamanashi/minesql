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
// (DeleteMark が設定されているレコードはスキップする)
//
// 戻り値: レコード (プライマリキー + 値), データがあるかどうか, エラー
func (ri *ClusteredIndexIterator) Next() ([][]byte, bool, error) {
	for {
		btrRecord, ok, err := ri.iterator.Next(ri.bp)
		if !ok {
			return nil, false, nil
		}
		if err != nil {
			return nil, false, err
		}

		// DeleteMark チェック: ソフトデリート済みならスキップ
		if len(btrRecord.HeaderBytes()) > 0 && btrRecord.HeaderBytes()[0] == 1 {
			continue
		}

		// レコード (プライマリキー + NonKey) をデコード
		var record [][]byte
		memcomparable.Decode(btrRecord.KeyBytes(), &record)
		memcomparable.Decode(btrRecord.NonKeyBytes(), &record)

		return record, true, nil
	}
}
