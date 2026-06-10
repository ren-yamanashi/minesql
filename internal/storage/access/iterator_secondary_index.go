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

		pi := NewPrimaryIndexIterator(iter, si.catalog, si.bufferPool, si.primaryTree.MetaPageId().FileId(), si.readView, si.undoLog)
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

// nextVisibleSecondaryRecord は次の可視セカンダリレコードを返す
//   - readView が nil の場合は deleteMark のみで判定する
//   - readView が非 nil の場合は lastTrxId で可視性判定し、不可視時は PK 経路フォールバックを行う
func (si *SecondaryIndexIterator) nextVisibleSecondaryRecord() (*SecondaryRecord, error) {
	for {
		record, ok, err := si.iterator.Next()
		if err != nil {
			return nil, err
		}
		if !ok {
			return nil, nil //nolint:nilnil // nil は、データなしを表す
		}

		secRec, err := DecodeSecondaryRecord(record, si.catalog, si.bufferPool, si.primaryTree.MetaPageId().FileId(), si.indexName)
		if err != nil {
			return nil, err
		}

		visible := si.readView == nil || si.readView.isVisible(secRec.lastTrxId)
		if visible && secRec.deleteMark == 1 {
			continue
		}
		if visible {
			return secRec, nil
		}

		// 不可視 → PK 経路フォールバック
		resolved, err := si.resolveViaPrimary(secRec)
		if err != nil {
			return nil, err
		}
		if resolved == nil {
			continue
		}
		return resolved, nil
	}
}

// resolveViaPrimary は PK 経路フォールバックでセカンダリレコードの可視性を解決する
//   - 一致時: secRec を返す (Read View から見て SK が一致するバージョンが存在)
//   - 不一致時 / チェーン終端時 / PK レコード不在時: nil を返す
func (si *SecondaryIndexIterator) resolveViaPrimary(secRec *SecondaryRecord) (*SecondaryRecord, error) {
	mtr := buffer.NewMtr(si.bufferPool)
	defer mtr.UnpinAll()

	primaryFileId := si.primaryTree.MetaPageId().FileId()
	iter, err := si.primaryTree.Search(mtr, SearchModeKey{Key: stringToByteSlice(secRec.pk)}.Encode())
	if err != nil {
		return nil, err
	}
	defer iter.Close()

	rec, ok, err := iter.Next()
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, nil //nolint:nilnil // nil は「該当の PK レコードが存在しない」を表す
	}

	current, err := DecodePrimaryRecord(rec, si.catalog, si.bufferPool, primaryFileId)
	if err != nil {
		return nil, err
	}

	for !si.readView.isVisible(current.lastTrxId) {
		prev, err := resolvePrevVersion(resolvePrevVersionInput{
			mtr:        mtr,
			undoLog:    si.undoLog,
			catalog:    si.catalog,
			bufferPool: si.bufferPool,
			fileId:     primaryFileId,
			record:     current,
		})
		if err != nil {
			return nil, err
		}
		if prev == nil {
			return nil, nil //nolint:nilnil // nil は「Read View から見て存在しない」を表す
		}
		current = prev
	}

	skMatched, err := si.matchSecondaryKey(current, secRec)
	if err != nil {
		return nil, err
	}
	if !skMatched {
		return nil, nil //nolint:nilnil // nil は「Read View から見て SK が一致しない」を表す
	}
	return secRec, nil
}

// matchSecondaryKey は PrimaryRecord と SecondaryRecord で SK 値が一致するかを判定する
func (si *SecondaryIndexIterator) matchSecondaryKey(pr *PrimaryRecord, secRec *SecondaryRecord) (bool, error) {
	primaryFileId := si.primaryTree.MetaPageId().FileId()
	index, err := fetchIndex(si.catalog, si.bufferPool, primaryFileId, si.indexName)
	if err != nil {
		return false, err
	}
	keyCols, err := fetchIndexKeyColumn(si.catalog, si.bufferPool, index.IndexId())
	if err != nil {
		return false, err
	}

	valMap := make(map[string]string, len(pr.colNames))
	for i, name := range pr.colNames {
		valMap[name] = pr.values[i]
	}

	skValues := make([]string, len(keyCols))
	for name, pos := range keyCols {
		skValues[pos] = valMap[name]
	}

	if len(skValues) != len(secRec.values) {
		return false, nil
	}
	for i := range skValues {
		if skValues[i] != secRec.values[i] {
			return false, nil
		}
	}
	return true, nil
}
