package access

import (
	"github.com/ren-yamanashi/minesql/internal/storage/btree"
	"github.com/ren-yamanashi/minesql/internal/storage/encode"
)

// SearchResult はインデックス検索の結果
type SearchResult struct {
	SecondaryKey [][]byte // デコード済みセカンダリキー
	PKValues     [][]byte // デコード済みプライマリキー (index-only scan 用)
	Record       [][]byte // デコード済みレコード (プライマリキー + その他カラム値)
}

// SecondaryIterator はセカンダリインデックスを辿るイテレータ
type SecondaryIterator struct {
	iterator     *btree.Iterator
	primaryBtree *btree.Btree // プライマリインデックスの B+Tree
	pkCount      int          // PrimaryKey のカラム数
	skCount      int          // SecondaryKey のカラム数
}

func newSecondaryIterator(
	iter *btree.Iterator,
	pb *btree.Btree,
	pkCount int,
	skCount int,
) *SecondaryIterator {
	return &SecondaryIterator{
		iterator:     iter,
		primaryBtree: pb,
		pkCount:      pkCount,
		skCount:      skCount,
	}
}

// Next はセカンダリインデックスから次の結果を返す
// (secondary-index -> primary-index の順で検索する)
//   - return: 検索結果, データがあるか
func (si *SecondaryIterator) Next() (*SearchResult, bool, error) {
	sk, encodedPk, err := si.nextVisibleSecondaryRecord()
	if err != nil {
		return nil, false, err
	}
	if sk == nil {
		return nil, false, nil
	}

	// PrimaryIterator を使用してレコード検索
	iter, err := si.primaryBtree.Search(btree.SearchModeKey{Key: encodedPk})
	if err != nil {
		return nil, false, err
	}

	pi := newPrimaryIterator(iter)
	result, found, err := pi.Next()
	if err != nil {
		return nil, false, err
	}
	if !found {
		return nil, false, nil
	}

	return &SearchResult{
		SecondaryKey: sk,
		Record:       result,
	}, true, nil
}

// NextIndexOnly はセカンダリインデックスのみを検索して次の結果を返す
//   - return: 検索結果, データがあるか
func (si *SecondaryIterator) NextIndexOnly() (*SearchResult, bool, error) {
	sk, encodedPk, err := si.nextVisibleSecondaryRecord()
	if err != nil {
		return nil, false, err
	}
	if sk == nil {
		return nil, false, nil
	}

	var pk [][]byte
	encode.Decode(encodedPk, &pk)

	return &SearchResult{
		SecondaryKey: sk,
		PKValues:     pk,
	}, true, nil
}

// nextVisibleSecondaryRecord は削除済みレコードをスキップして次の可視セカンダリレコードを返す
//   - return:
//   - decodedSk: デコード済みのセカンダリキー (データがない場合は nil)
//   - encodedPk: 未デコード (エンコード済み) のプライマリキー
func (si *SecondaryIterator) nextVisibleSecondaryRecord() (decodedSk [][]byte, encodedPk []byte, err error) {
	for {
		record, ok, err := si.iterator.Next()
		if err != nil {
			return nil, nil, err
		}
		if !ok {
			return nil, nil, nil
		}

		deleteMark := record.Header()[0]
		if deleteMark == 1 {
			continue
		}

		sk, encodedPk := encode.DecodeFirstN(record.Key(), si.skCount)
		return sk, encodedPk, nil
	}
}
