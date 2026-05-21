package access

import (
	"github.com/ren-yamanashi/minesql/internal/storage/btree"
	"github.com/ren-yamanashi/minesql/internal/storage/catalog"
	"github.com/ren-yamanashi/minesql/internal/storage/page"
)

// primaryIterator はプライマリインデックスを辿るイテレータ
type primaryIterator struct {
	iterator *btree.Iterator
	catalog  *catalog.Catalog
	fileId   page.FileId
}

func newPrimaryIterator(iter *btree.Iterator, ct *catalog.Catalog, fileId page.FileId) *primaryIterator {
	return &primaryIterator{
		fileId:   fileId,
		catalog:  ct,
		iterator: iter,
	}
}

// next はデコード済みの次の可視レコードを返す
//   - return: レコード, データがあるか
func (pi *primaryIterator) next() (*primaryRecord, bool, error) {
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

		result, err := decodePrimaryRecord(record, pi.catalog, pi.fileId)
		if err != nil {
			return nil, false, err
		}
		return result, true, nil
	}
}
