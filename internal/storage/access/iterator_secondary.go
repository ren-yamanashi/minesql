package access

import (
	"github.com/ren-yamanashi/minesql/internal/storage/btree"
	"github.com/ren-yamanashi/minesql/internal/storage/catalog"
)

// SearchResult はインデックス検索の結果
type SearchResult struct {
	SecondaryKey [][]byte // デコード済みセカンダリキー
	PKValues     [][]byte // デコード済みプライマリキー (index-only scan 用)
	Record       [][]byte // デコード済みレコード (プライマリキー + その他カラム値)
}

// SecondaryIterator はセカンダリインデックスを辿るイテレータ
type SecondaryIterator struct {
	indexName   string
	iterator    *btree.Iterator
	catalog     *catalog.Catalog
	primaryTree *btree.Btree // プライマリインデックスの B+Tree
}

func newSecondaryIterator(
	indexName string,
	iter *btree.Iterator,
	ct *catalog.Catalog,
	pb *btree.Btree,
) *SecondaryIterator {
	return &SecondaryIterator{
		indexName:   indexName,
		iterator:    iter,
		catalog:     ct,
		primaryTree: pb,
	}
}

// Next はセカンダリインデックスから次の結果を返す
// (secondary-index -> primary-index の順で検索する)
//   - return: 検索結果, データがあるか
func (si *SecondaryIterator) Next() (*PrimaryRecord, bool, error) {
	secondaryRecord, err := si.nextVisibleSecondaryRecord()
	if err != nil {
		return nil, false, err
	}
	if secondaryRecord == nil {
		return nil, false, nil
	}

	// PrimaryIterator を使用してレコード検索
	iter, err := si.primaryTree.Search(SearchModeKey{Key: stringToByteSlice(secondaryRecord.Pk)}.encode())
	if err != nil {
		return nil, false, err
	}

	pi := newPrimaryIterator(iter, si.catalog, si.primaryTree.MetaPageId.FileId)
	result, found, err := pi.Next()
	if err != nil {
		return nil, false, err
	}
	if !found {
		return nil, false, nil
	}
	return result, true, nil
}

// NextIndexOnly はセカンダリインデックスのみを検索して次の結果を返す
//   - return: 検索結果, データがあるか
func (si *SecondaryIterator) NextIndexOnly() (*SecondaryRecord, bool, error) {
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
//   - return:
//   - decodedSk: デコード済みのセカンダリキー (データがない場合は nil)
//   - encodedPk: 未デコード (エンコード済み) のプライマリキー
func (si *SecondaryIterator) nextVisibleSecondaryRecord() (*SecondaryRecord, error) {
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

		return decodeSecondaryRecord(record, si.catalog, si.primaryTree.MetaPageId.FileId, si.indexName)
	}
}
