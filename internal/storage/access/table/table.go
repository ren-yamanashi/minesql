package table

import (
	"fmt"
	"minesql/internal/storage/access/btree"
	"minesql/internal/storage/access/btree/node"
	"minesql/internal/storage/bufferpool"
	"minesql/internal/storage/page"
)

type Table struct {
	// テーブル名
	Name string
	// テーブルの内容が入っている B+Tree のメタページの ID
	MetaPageId page.PageId
	// プライマリキーの列数 (プライマリキーは先頭から連続している想定)
	// 例: プライマリキーが (id, name) の場合、PrimaryKeyCount は 2 になる
	PrimaryKeyCount uint8
	// テーブルに紐づくユニークインデックス群
	UniqueIndexes []*UniqueIndex
}

func NewTable(name string, metaPageId page.PageId, primaryKeyCount uint8, uniqueIndexes []*UniqueIndex) Table {
	return Table{
		Name:            name,
		MetaPageId:      metaPageId,
		PrimaryKeyCount: primaryKeyCount,
		UniqueIndexes:   uniqueIndexes,
	}
}

// 空のテーブルを新規作成する
func (t *Table) Create(bpm *bufferpool.BufferPoolManager) error {
	// テーブルの B+Tree を作成
	tree, err := btree.CreateBTree(bpm, t.MetaPageId)
	if err != nil {
		return err
	}
	t.MetaPageId = tree.MetaPageId

	// ユニークインデックスを作成
	for _, ui := range t.UniqueIndexes {
		err = ui.Create(bpm, ui.MetaPageId)
		if err != nil {
			return err
		}
	}
	return nil
}

// テーブルに行を挿入する
// プライマリキーを key, 他のカラム値を value としたペアを作成し、B+Tree に挿入する
func (t *Table) Insert(bpm *bufferpool.BufferPoolManager, record [][]byte) error {
	btree := btree.NewBTree(t.MetaPageId)

	// キーをエンコード
	var encodedKey []byte
	Encode(record[:t.PrimaryKeyCount], &encodedKey)

	// 値をエンコード
	var encodedValue []byte
	Encode(record[t.PrimaryKeyCount:], &encodedValue)

	// B+Tree に挿入
	err := btree.Insert(bpm, node.NewPair(encodedKey, encodedValue))
	if err != nil {
		return err
	}

	// ユニークインデックスに挿入
	for _, ui := range t.UniqueIndexes {
		err := ui.Insert(bpm, encodedKey, record)
		if err != nil {
			return err
		}
	}

	return nil
}

// インデックス名からユニークインデックスを取得する
func (t *Table) GetUniqueIndexByName(indexName string) (*UniqueIndex, error) {
	for _, ui := range t.UniqueIndexes {
		if ui.Name == indexName {
			return ui, nil
		}
	}
	return nil, fmt.Errorf("unique index %s not found in table %s", indexName, t.Name)
}
