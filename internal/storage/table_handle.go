package storage

import (
	"fmt"
	"minesql/internal/storage/access/btree"
	"minesql/internal/storage/access/btree/node"
	"minesql/internal/storage/access/table"
	"minesql/internal/storage/bufferpool"
)

// テーブル操作の窓口
type TableHandler struct {
	// スキャン用のイテレータ
	iterator          *btree.Iterator
	table             *table.Table
	bufferPoolManager *bufferpool.BufferPoolManager
}

func NewTableHandle(tbl *table.Table, bpm *bufferpool.BufferPoolManager) *TableHandler {
	return &TableHandler{
		iterator:          nil, // 初期化時点ではイテレータは設定しない。`SetXxxIterator` メソッドで設定する
		table:             tbl,
		bufferPoolManager: bpm,
	}
}

// テーブルスキャン用のイテレータを設定
func (th *TableHandler) SetTableIterator(searchMode btree.SearchMode) error {
	btr := btree.NewBTree(th.table.MetaPageId)
	iterator, err := btr.Search(th.bufferPoolManager, searchMode)
	if err != nil {
		return err
	}
	th.iterator = iterator
	return nil
}

// インデックススキャン用のイテレータを設定
func (th *TableHandler) SetIndexIterator(indexName string, searchMode btree.SearchMode) error {
	// インデックスを検索
	var indexBTree *btree.BTree
	for _, idx := range th.table.UniqueIndexes {
		if idx.Name == indexName {
			indexBTree = btree.NewBTree(idx.MetaPageId)
			break
		}
	}

	// インデックスが見つからなかった場合はエラー
	if indexBTree == nil {
		return fmt.Errorf("index %s not found in table %s", indexName, th.table.Name)
	}

	// インデックスの B+Tree で検索を行うイテレータを作成
	iterator, err := indexBTree.Search(th.bufferPoolManager, searchMode)
	if err != nil {
		return err
	}

	th.iterator = iterator
	return nil
}

// プライマリキー検索用のイテレータを設定
func (th *TableHandler) SetSearchPrimaryKeyIterator(encodedKey []byte) error {
	// テーブルの B+Tree を開き、プライマリキーで検索
	tableBTree := btree.NewBTree(th.table.MetaPageId)
	iterator, err := tableBTree.Search(th.bufferPoolManager, btree.SearchModeKey{Key: encodedKey})
	if err != nil {
		return err
	}
	th.iterator = iterator
	return nil
}

// Next は次のペアを取得する
func (th *TableHandler) Next() (node.Pair, bool, error) {
	return th.iterator.Next(th.bufferPoolManager)
}
