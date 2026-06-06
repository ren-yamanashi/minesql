package access

import (
	"github.com/ren-yamanashi/minesql/internal/storage/btree"
	"github.com/ren-yamanashi/minesql/internal/storage/buffer"
	"github.com/ren-yamanashi/minesql/internal/storage/dictionary"
	"github.com/ren-yamanashi/minesql/internal/storage/undo"
)

type SecondaryIndexIterator struct {
	indexName   string
	iterator    *btree.Iterator
	catalog     *dictionary.Catalog
	bufferPool  *buffer.Pool
	primaryTree *btree.Tree // プライマリインデックスの B+Tree
	readView    *readView   // nil の場合は可視性判定をスキップする
	undoLog     *undo.Manager
}

func NewSecondaryIndexIterator(
	indexName string,
	iter *btree.Iterator,
	ct *dictionary.Catalog,
	bp *buffer.Pool,
	pt *btree.Tree,
	readView *readView,
	undoLog *undo.Manager,
) *SecondaryIndexIterator {
	return &SecondaryIndexIterator{
		indexName:   indexName,
		iterator:    iter,
		catalog:     ct,
		bufferPool:  bp,
		primaryTree: pt,
		readView:    readView,
		undoLog:     undoLog,
	}
}

func (si *SecondaryIndexIterator) Close() {
	si.iterator.Close()
}

// Next はセカンダリインデックスから次の結果を返す
// (secondary-index -> primary-index の順で検索する)
//   - return: 検索結果, データがあるか
func (si *SecondaryIndexIterator) Next() (*PrimaryRecord, bool, error) {
	for {
		secondaryRecord, err := si.nextVisibleSecondaryRecord()
		if err != nil {
			return nil, false, err
		}
		if secondaryRecord == nil {
			return nil, false, nil
		}

		// PrimaryIterator を使用してレコード検索
		mtr := buffer.NewMtr(si.bufferPool)
		iter, err := si.primaryTree.Search(mtr, SearchModeKey{Key: stringToByteSlice(secondaryRecord.pk)}.Encode())
		if err != nil {
			mtr.UnpinAll()
			return nil, false, err
		}

		pi := NewPrimaryIndexIterator(iter, si.catalog, si.bufferPool, si.primaryTree.MetaPageId().FileId(), si.readView, si.undoLog, mtr)
		result, found, err := pi.Next()
		pi.Close()
		mtr.UnpinAll()
		if err != nil {
			return nil, false, err
		}
		if found {
			return result, true, nil
		}
		// プライマリレコードが見つからない場合は次のセカンダリレコードに進む
	}
}

// NextIndexOnly はセカンダリインデックスのみを検索して次の結果を返す
//   - return: 検索結果, データがあるか
func (si *SecondaryIndexIterator) NextIndexOnly() (*SecondaryRecord, bool, error) {
	record, err := si.nextVisibleSecondaryRecord()
	if err != nil {
		return nil, false, err
	}
	if record == nil {
		return nil, false, nil
	}
	return record, true, nil
}

// nextVisibleSecondaryRecord は削除済みレコードをスキップして次の可視セカンダリレコードを返す
func (si *SecondaryIndexIterator) nextVisibleSecondaryRecord() (*SecondaryRecord, error) {
	for {
		record, ok, err := si.iterator.Next()
		if err != nil {
			return nil, err
		}
		if !ok {
			return nil, nil //nolint:nilnil // nil は、データなしを表す
		}

		deleteMark := record.Header()[0]
		if deleteMark == 1 {
			continue
		}

		return DecodeSecondaryRecord(record, si.catalog, si.bufferPool, si.primaryTree.MetaPageId().FileId(), si.indexName)
	}
}
