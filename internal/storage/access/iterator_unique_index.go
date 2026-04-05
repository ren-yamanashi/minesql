package access

import (
	"minesql/internal/storage/btree"
	"minesql/internal/storage/buffer"
	"minesql/internal/storage/encode"
)

// SearchResult はインデックス検索の結果
type SearchResult struct {
	UniqueKey [][]byte // デコード済みユニークキー
	Record    [][]byte // デコード済みレコード (プライマリキー + 値)
}

type UniqueIndexIterator struct {
	iterator   *btree.Iterator    // ユニークインデックスの B+Tree イテレータ
	tableBTree *btree.BTree       // テーブル本体の B+Tree (インデックス検索 -> テーブル検索の流れで検索を行うために保持)
	bp         *buffer.BufferPool // バッファプール
	pkCount    uint8              // PK のカラム数
}

func newUniqueIndexIterator(iterator *btree.Iterator, tableBTree *btree.BTree, bp *buffer.BufferPool, pkCount uint8) *UniqueIndexIterator {
	return &UniqueIndexIterator{
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
func (uii *UniqueIndexIterator) Next() (*SearchResult, bool, error) {
	for {
		// ユニークインデックスから次のレコードを取得
		indexRecord, ok, err := uii.iterator.Next(uii.bp)
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

		// Key = concat(encodedUK, encodedPK) から先頭のユニークキーだけをデコードし、
		// 残りのエンコード済み PK バイト列はそのままテーブル検索に使う (再エンコード不要)
		uniqueKey, encodedPK := encode.DecodeFirstN(indexRecord.KeyBytes(), 1)

		// テーブル本体を検索してレコードを取得
		tableIterator, err := uii.tableBTree.Search(uii.bp, btree.SearchModeKey{Key: encodedPK})
		if err != nil {
			return nil, false, err
		}
		tableRecord, ok, err := tableIterator.Next(uii.bp)
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

		return &SearchResult{
			UniqueKey: uniqueKey,
			Record:    record,
		}, true, nil
	}
}
