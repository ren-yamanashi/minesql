package executor

import (
	"minesql/internal/storage/access/btree"
	"minesql/internal/storage/access/table"
	"minesql/internal/storage/bufferpool"
	"minesql/internal/storage/disk"
)

type IndexScan struct {
	tableBtree *btree.BTree
	// インデックスを走査するイテレータ
	indexIterator *btree.Iterator
	// 継続条件を満たすかどうかを判定する関数
	WhileCondition func(record Record) bool
}

func NewIndexScan(
	tableMetaPageId disk.OldPageId,
	indexIterator *btree.Iterator,
	whileCondition func(record Record) bool,
) *IndexScan {
	return &IndexScan{
		tableBtree:     btree.NewBTree(tableMetaPageId),
		indexIterator:  indexIterator,
		WhileCondition: whileCondition,
	}
}

// 次の Record を取得する
// データがない場合、継続条件を満たさない場合は (nil, nil) を返す
func (is *IndexScan) Next(bpm *bufferpool.BufferPoolManager) (Record, error) {
	secondaryIndexPair, ok, err := is.indexIterator.Next(bpm)
	if !ok {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}

	// セカンダリキーをデコード
	var secondaryKey [][]byte
	table.Decode(secondaryIndexPair.Key, &secondaryKey)

	// 継続条件をチェック
	if !is.WhileCondition(secondaryKey) {
		return nil, nil
	}

	// テーブルの B+Tree を検索
	tableIterator, err := is.tableBtree.Search(bpm, btree.SearchModeKey{Key: secondaryIndexPair.Value})
	if err != nil {
		return nil, err
	}
	tablePair, ok, err := tableIterator.Next(bpm)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, nil
	}

	// レコード (プライマリキー + 値) をデコード
	var record [][]byte
	table.Decode(tablePair.Key, &record)
	table.Decode(tablePair.Value, &record)
	return record, nil
}
