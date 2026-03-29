package access

import (
	"minesql/internal/btree"
	"minesql/internal/encode"
	"minesql/internal/storage"
)

// SecondaryIndexSearchResult はインデックス検索の結果を表す
type SecondaryIndexSearchResult struct {
	SecondaryKey [][]byte // デコード済みセカンダリキー
	Record       [][]byte // デコード済みテーブルレコード (プライマリキー + 値)
}

type SecondaryIndexIterator struct {
	indexIterator   *btree.Iterator
	tableBTree      *btree.BPlusTree
	bp              *storage.BufferPool
	primaryKeyCount uint8 // PK のカラム数 (Key からセカンダリキーと PK を分離するために必要)
}

func newSecondaryIndexIterator(indexIterator *btree.Iterator, tableBTree *btree.BPlusTree, bp *storage.BufferPool, primaryKeyCount uint8) *SecondaryIndexIterator {
	return &SecondaryIndexIterator{
		indexIterator:   indexIterator,
		tableBTree:      tableBTree,
		bp:              bp,
		primaryKeyCount: primaryKeyCount,
	}
}

// Next はインデックスから次の結果を返す
//
// インデックスから次のレコードを取得し、Key = concat(encodedSecondaryKey, encodedPK) を分離した後、
// PK でテーブル本体を検索してレコードをデコードする
//
// DeleteMark が設定されているレコードはスキップする
func (iri *SecondaryIndexIterator) Next() (*SecondaryIndexSearchResult, bool, error) {
	for {
		// セカンダリインデックスから次のレコードを取得
		indexRecord, ok, err := iri.indexIterator.Next(iri.bp)
		if !ok {
			return nil, false, nil
		}
		if err != nil {
			return nil, false, err
		}

		// DeleteMark チェック: ソフトデリート済みならスキップ
		if len(indexRecord.HeaderBytes()) > 0 && indexRecord.HeaderBytes()[0] == 1 {
			continue
		}

		// Key = concat(encodedSecondaryKey, encodedPK) を分離
		// encode.Decode で全カラムに分離する
		var keyColumns [][]byte
		encode.Decode(indexRecord.KeyBytes(), &keyColumns)

		// セカンダリキー = 先頭カラム、PK = 残りのカラム
		secondaryKey := keyColumns[:1]
		pkColumns := keyColumns[1:]

		// PK カラムを再エンコードしてテーブル本体を検索
		var encodedPK []byte
		encode.Encode(pkColumns, &encodedPK)

		tableIterator, err := iri.tableBTree.Search(iri.bp, btree.SearchModeKey{Key: encodedPK})
		if err != nil {
			return nil, false, err
		}
		tableRecord, ok, err := tableIterator.Next(iri.bp)
		if err != nil {
			return nil, false, err
		}
		if !ok {
			return nil, false, nil
		}

		// テーブルレコード (プライマリキー + NonKey) をデコード
		var record [][]byte
		encode.Decode(tableRecord.KeyBytes(), &record)
		encode.Decode(tableRecord.NonKeyBytes(), &record)

		return &SecondaryIndexSearchResult{
			SecondaryKey: secondaryKey,
			Record:       record,
		}, true, nil
	}
}
