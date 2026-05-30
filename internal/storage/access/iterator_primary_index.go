package access

import (
	"github.com/ren-yamanashi/minesql/internal/storage/btree"
	"github.com/ren-yamanashi/minesql/internal/storage/buffer"
	"github.com/ren-yamanashi/minesql/internal/storage/dictionary"
	"github.com/ren-yamanashi/minesql/internal/storage/page"
)

type PrimaryIndexIterator struct {
	iterator   *btree.Iterator
	catalog    *dictionary.Catalog
	bufferPool *buffer.Pool
	fileId     page.FileId
}

func NewPrimaryIndexIterator(iter *btree.Iterator, ct *dictionary.Catalog, bp *buffer.Pool, fileId page.FileId) *PrimaryIndexIterator {
	return &PrimaryIndexIterator{
		fileId:     fileId,
		catalog:    ct,
		bufferPool: bp,
		iterator:   iter,
	}
}

func (pi *PrimaryIndexIterator) Close() {
	pi.iterator.Close()
}

// Next はデコード済みの次の可視レコードを返す
//   - return: レコード, データがあるか
func (pi *PrimaryIndexIterator) Next() (*PrimaryRecord, bool, error) {
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

		result, err := DecodePrimaryRecord(record, pi.catalog, pi.bufferPool, pi.fileId)
		if err != nil {
			return nil, false, err
		}
		return result, true, nil
	}
}
