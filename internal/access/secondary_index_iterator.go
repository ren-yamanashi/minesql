package access

import (
	"minesql/internal/storage/btree"
	"minesql/internal/storage/bufferpool"
	"minesql/internal/storage/memcomparable"
)

// SecondaryIndexSearchResult はインデックス検索の結果を表す
type SecondaryIndexSearchResult struct {
	SecondaryKey [][]byte // デコード済みセカンダリキー
	Record       [][]byte // デコード済みテーブルレコード (プライマリキー + 値)
}

type SecondaryIndexIterator struct {
	indexIterator *btree.Iterator
	tableBTree    *btree.BPlusTree
	bp            *bufferpool.BufferPool
}

func newSecondaryIndexIterator(indexIterator *btree.Iterator, tableBTree *btree.BPlusTree, bp *bufferpool.BufferPool) *SecondaryIndexIterator {
	return &SecondaryIndexIterator{
		indexIterator: indexIterator,
		tableBTree:    tableBTree,
		bp:            bp,
	}
}

// Next はインデックスから次の結果を返す
//
// インデックスから次のペアを取得し、セカンダリキーをデコードした後、
// エンコード済みプライマリキーでテーブル本体を検索してレコードをデコードする
func (iri *SecondaryIndexIterator) Next() (*SecondaryIndexSearchResult, bool, error) {
	// セカンダリインデックスから次のペアを取得
	secondaryIndexPair, ok, err := iri.indexIterator.Next(iri.bp)
	if !ok {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, err
	}

	// セカンダリキーをデコード
	var secondaryKey [][]byte
	memcomparable.Decode(secondaryIndexPair.Key, &secondaryKey)

	// エンコード済みプライマリキーでテーブル本体を検索
	tableIterator, err := iri.tableBTree.Search(iri.bp, btree.SearchModeKey{Key: secondaryIndexPair.Value})
	if err != nil {
		return nil, false, err
	}
	tablePair, ok, err := tableIterator.Next(iri.bp)
	if err != nil {
		return nil, false, err
	}
	if !ok {
		return nil, false, nil
	}

	// レコード (プライマリキー + 値) をデコード
	var record [][]byte
	memcomparable.Decode(tablePair.Key, &record)
	memcomparable.Decode(tablePair.Value, &record)

	return &SecondaryIndexSearchResult{
		SecondaryKey: secondaryKey,
		Record:       record,
	}, true, nil
}
