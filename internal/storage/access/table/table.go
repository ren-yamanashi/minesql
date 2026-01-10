package table

import (
	"minesql/internal/storage/access/btree"
	"minesql/internal/storage/access/btree/node"
	"minesql/internal/storage/bufferpool"
	"minesql/internal/storage/disk"
)

type Table struct {
	// テーブルの内容が入っている B+Tree のメタページの ID
	MetaPageId disk.OldPageId
	// プライマリキーの列数 (プライマリキーは先頭から連続している想定)
	// 例: プライマリキーが (id, name) の場合、PrimaryKeyCount は 2 になる
	PrimaryKeyCount int
	// テーブルに紐づくユニークインデックス群
	UniqueIndexes []*UniqueIndex
}

func NewTable(metaPageId disk.OldPageId, primaryKeyCount int, uniqueIndexes []*UniqueIndex) Table {
	return Table{
		MetaPageId:      metaPageId,
		PrimaryKeyCount: primaryKeyCount,
		UniqueIndexes:   uniqueIndexes,
	}
}

// 空のテーブルを新規作成する
func (t *Table) Create(bpm *bufferpool.BufferPoolManager) error {
	// テーブルの B+Tree を作成
	tree, err := btree.CreateBTree(bpm)
	if err != nil {
		return err
	}
	t.MetaPageId = tree.MetaPageId

	// ユニークインデックスを作成
	for _, ui := range t.UniqueIndexes {
		err := ui.Create(bpm)
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
