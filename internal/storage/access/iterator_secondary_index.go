package access

import (
	"bytes"
	"errors"

	"github.com/ren-yamanashi/minesql/internal/storage/btree"
	"github.com/ren-yamanashi/minesql/internal/storage/buffer"
	"github.com/ren-yamanashi/minesql/internal/storage/dictionary"
	"github.com/ren-yamanashi/minesql/internal/storage/encode"
	"github.com/ren-yamanashi/minesql/internal/storage/undo"
)

type SecondaryIndexIterator struct {
	indexName   string
	iterator    *btree.ScanIterator
	catalog     *dictionary.Catalog
	bufferPool  *buffer.Pool
	primaryTree *btree.Tree // プライマリインデックスの B+Tree
	readView    *readView   // nil の場合は可視性判定をスキップする
	undoLog     *undo.Manager
}

func NewSecondaryIndexIterator(
	indexName string,
	iter *btree.ScanIterator,
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

// Close は走査を途中で打ち切るときに呼ぶ (終端到達時は自動解放されるため省略可)
func (si *SecondaryIndexIterator) Close() {
	si.iterator.Close()
}

// Next はセカンダリインデックスから次の結果を返す
// (secondary-index -> primary-index の順で検索する)
//   - return: 検索結果, データがあるか
func (si *SecondaryIndexIterator) Next() (*PrimaryRecord, bool, error) {
	for {
		rec, mtr, ok, err := si.iterator.Next()
		if err != nil {
			return nil, false, err
		}
		if !ok {
			return nil, false, nil
		}
		result, done, err := si.resolveNext(rec, mtr)
		mtr.UnpinAll()
		if err != nil {
			return nil, false, err
		}
		if done {
			return result, true, nil
		}
	}
}

// resolveNext は 1 レコードについてデコード → プライマリ経由の解決までを 1 mtr 内で行う
//   - done=true のとき result が可視の PrimaryRecord、done=false のときは continue
func (si *SecondaryIndexIterator) resolveNext(rec btree.Record, mtr *buffer.Mtr) (*PrimaryRecord, bool, error) {
	secRec, err := DecodeSecondaryRecord(rec, si.catalog, si.bufferPool, si.primaryTree.MetaPageId().FileId(), si.indexName)
	if err != nil {
		return nil, false, err
	}
	if si.readView == nil && secRec.deleteMark == 1 {
		return nil, false, nil
	}

	result, err := si.resolvePrimaryVersion(secRec, mtr)
	if err != nil {
		return nil, false, err
	}
	if result == nil {
		return nil, false, nil
	}
	return result, true, nil
}

// resolvePrimaryVersion はセカンダリレコードのプライマリキーで本体レコードを引き、Read View から見えるバージョンを返す
//   - readView が nil の場合は deleteMark のみで判定する
//   - 本体レコード不在 / 可視バージョンなし / 削除済み / SK 不一致の場合は nil を返す
func (si *SecondaryIndexIterator) resolvePrimaryVersion(secRec *SecondaryRecord, mtr *buffer.Mtr) (*PrimaryRecord, error) {
	primaryFileId := si.primaryTree.MetaPageId().FileId()
	pkKey := encode.Encode(nil, stringToByteSlice(secRec.pk))
	iter, err := si.primaryTree.Search(mtr, btree.SearchModeKey{Key: pkKey})
	if err != nil {
		return nil, err
	}
	defer mtr.Unpin(iter.BufferPageId())

	rec, ok, err := iter.Get()
	if err != nil {
		return nil, err
	}
	if !ok || !bytes.Equal(rec.Key(), pkKey) {
		return nil, nil //nolint:nilnil // nil は「該当の PK レコードが存在しない」を表す
	}

	current, err := DecodePrimaryRecord(rec, si.catalog, si.bufferPool, primaryFileId)
	if err != nil {
		return nil, err
	}
	if si.readView == nil {
		if current.deleteMark == 1 {
			return nil, nil //nolint:nilnil // nil は「削除済み」を表す
		}
		return current, nil
	}

	ascended := false
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
		ascended = true
	}
	if current.deleteMark == 1 {
		return nil, nil //nolint:nilnil // nil は「Read View から見て削除済み」を表す
	}

	if !ascended && secRec.deleteMark == 0 {
		return current, nil
	}
	skMatched, err := si.matchSecondaryKey(current, secRec)
	if err != nil {
		return nil, err
	}
	if !skMatched {
		return nil, nil //nolint:nilnil // nil は「Read View から見て SK が一致しない」を表す
	}
	return current, nil
}

// NextIndexOnly はセカンダリインデックスのみを検索して次の結果を返す
//   - return: 検索結果, データがあるか
func (si *SecondaryIndexIterator) NextIndexOnly() (*SecondaryRecord, bool, error) {
	for {
		rec, mtr, ok, err := si.iterator.Next()
		if err != nil {
			return nil, false, err
		}
		if !ok {
			return nil, false, nil
		}
		result, done, err := si.resolveNextIndexOnly(rec, mtr)
		mtr.UnpinAll()
		if err != nil {
			return nil, false, err
		}
		if done {
			return result, true, nil
		}
	}
}

// resolveNextIndexOnly は 1 レコードについて index-only 経路の解決を 1 mtr 内で行う
//   - done=true のとき result が可視の SecondaryRecord、done=false のときは continue
func (si *SecondaryIndexIterator) resolveNextIndexOnly(rec btree.Record, mtr *buffer.Mtr) (*SecondaryRecord, bool, error) {
	secRec, err := DecodeSecondaryRecord(rec, si.catalog, si.bufferPool, si.primaryTree.MetaPageId().FileId(), si.indexName)
	if err != nil {
		return nil, false, err
	}

	visible := si.readView == nil || si.readView.isVisible(secRec.lastTrxId)
	if visible && secRec.deleteMark == 1 {
		return nil, false, nil
	}
	if visible {
		return secRec, true, nil
	}

	// 不可視 → PK 経路フォールバック
	resolved, err := si.resolveViaPrimary(secRec, mtr)
	if err != nil {
		return nil, false, err
	}
	if resolved == nil {
		return nil, false, nil
	}
	return resolved, true, nil
}

// resolveViaPrimary は PK 経路フォールバックでセカンダリレコードの可視性を解決する
//   - 一致時: secRec を返す (Read View から見て SK が一致するバージョンが存在)
//   - 不一致時 / チェーン終端時 / PK レコード不在時: nil を返す
func (si *SecondaryIndexIterator) resolveViaPrimary(secRec *SecondaryRecord, mtr *buffer.Mtr) (*SecondaryRecord, error) {
	primaryFileId := si.primaryTree.MetaPageId().FileId()
	pkKey := encode.Encode(nil, stringToByteSlice(secRec.pk))
	rec, position, err := si.primaryTree.FindByKey(mtr, pkKey)
	if errors.Is(err, btree.ErrKeyNotFound) {
		return nil, nil //nolint:nilnil // nil は「該当の PK レコードが存在しない」を表す
	}
	if err != nil {
		return nil, err
	}
	defer mtr.Unpin(position.PageId)

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
