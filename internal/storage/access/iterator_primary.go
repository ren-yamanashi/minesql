package access

import (
	"github.com/ren-yamanashi/minesql/internal/storage/btree"
	"github.com/ren-yamanashi/minesql/internal/storage/encode"
)

// PrimaryIterator はプライマリインデックスを辿るイテレータ
type PrimaryIterator struct {
	iterator *btree.Iterator
}

func newPrimaryIterator(iter *btree.Iterator) *PrimaryIterator {
	return &PrimaryIterator{
		iterator: iter,
	}
}

// Next はデコード済みの次の可視レコードを返す
//   - return: レコード, データがあるか
func (pi *PrimaryIterator) Next() ([][]byte, bool, error) {
	for {
		record, ok, err := pi.iterator.Next()
		if err != nil {
			return nil, false, err
		}
		if !ok {
			return nil, false, nil
		}

		deleteMark := record.Header()[0]
		if deleteMark == 1 {
			continue
		}

		var result [][]byte
		encode.Decode(record.Key(), &result)
		encode.Decode(record.NonKey(), &result)
		return result, true, nil
	}
}
