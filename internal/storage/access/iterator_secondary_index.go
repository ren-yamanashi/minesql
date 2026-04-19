package access

import (
	"minesql/internal/storage/btree"
	"minesql/internal/storage/buffer"
	"minesql/internal/storage/encode"
)

// SearchResult はインデックス検索の結果
type SearchResult struct {
	SecondaryKey [][]byte // デコード済みセカンダリキー
	PKValues     [][]byte // デコード済みプライマリキー (index-only scan 用)
	Record       [][]byte // デコード済みレコード (プライマリキー + 値)
}

type SecondaryIndexIterator struct {
	iterator   *btree.Iterator    // セカンダリインデックスの B+Tree イテレータ
	tableBTree *btree.BTree       // テーブル本体の B+Tree (インデックス検索 → テーブル検索の流れで使用)
	bp         *buffer.BufferPool // バッファプール
	pkCount    uint8              // PK のカラム数
}

func newSecondaryIndexIterator(iterator *btree.Iterator, tableBTree *btree.BTree, bp *buffer.BufferPool, pkCount uint8) *SecondaryIndexIterator {
	return &SecondaryIndexIterator{
		iterator:   iterator,
		tableBTree: tableBTree,
		bp:         bp,
		pkCount:    pkCount,
	}
}

// Next はインデックスから次の結果を返す
// (DeleteMark が設定されているレコードはスキップする)
//
// インデックスから次のレコードを取得し、PK でテーブル本体を検索してレコードをデコードする
func (sii *SecondaryIndexIterator) Next() (*SearchResult, bool, error) {
	for {
		// セカンダリインデックスから次のレコードを取得
		indexRecord, ok, err := sii.iterator.Next(sii.bp)
		if !ok {
			return nil, false, nil
		}
		if err != nil {
			return nil, false, err
		}

		// DeleteMark が 1 のレコードはスキップ
		if len(indexRecord.HeaderBytes()) > 0 && indexRecord.HeaderBytes()[0] == 1 {
			continue
		}

		// Key = concat(encodedSecKey, encodedPK) から先頭のセカンダリキーだけをデコードし、
		// 残りのエンコード済み PK バイト列はそのままテーブル検索に使う (再エンコード不要)
		secondaryKey, encodedPK := encode.DecodeFirstN(indexRecord.KeyBytes(), 1)

		// テーブル本体を検索してレコードを取得
		tableIterator, err := sii.tableBTree.Search(sii.bp, btree.SearchModeKey{Key: encodedPK})
		if err != nil {
			return nil, false, err
		}
		tableRecord, ok, err := tableIterator.Next(sii.bp)
		if err != nil {
			return nil, false, err
		}
		if !ok {
			return nil, false, nil
		}

		// テーブルレコード (プライマリキー + NonKey) をデコード
		var record [][]byte
		encode.Decode(tableRecord.KeyBytes(), &record)
		_, _, nonKeyColumns := decodeRecordNonKey(tableRecord.NonKeyBytes())
		encode.Decode(nonKeyColumns, &record)

		return &SearchResult{
			SecondaryKey: secondaryKey,
			Record:       record,
		}, true, nil
	}
}

// NextIndexOnly はインデックスデータのみから結果を返す (テーブル本体の検索なし)
//
// PK とセカンダリキーをインデックスキーからデコードして返す
func (sii *SecondaryIndexIterator) NextIndexOnly() (*SearchResult, bool, error) {
	for {
		indexRecord, ok, err := sii.iterator.Next(sii.bp)
		if !ok {
			return nil, false, nil
		}
		if err != nil {
			return nil, false, err
		}

		// DeleteMark が 1 のレコードはスキップ
		if len(indexRecord.HeaderBytes()) > 0 && indexRecord.HeaderBytes()[0] == 1 {
			continue
		}

		// Key = concat(encodedSecKey, encodedPK) からセカンダリキーと PK をデコード
		secondaryKey, encodedPK := encode.DecodeFirstN(indexRecord.KeyBytes(), 1)

		var pkValues [][]byte
		encode.Decode(encodedPK, &pkValues)

		return &SearchResult{
			SecondaryKey: secondaryKey,
			PKValues:     pkValues,
		}, true, nil
	}
}
