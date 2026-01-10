package table

import (
	"minesql/internal/storage/access/btree"
	"minesql/internal/storage/access/btree/node"
	"minesql/internal/storage/bufferpool"
	"minesql/internal/storage/disk"
)

type SimpleTable struct {
	// テーブルの内容が入っている B+Tree のメタページの ID
	MetaPageId disk.PageId
	// プライマリキーの列数 (プライマリキーは先頭から連続している想定)
	// 例: プライマリキーが (id, name) の場合、PrimaryKeyCount は 2 になる
	PrimaryKeyCount int
}

// 空のテーブルを新規作成する
func (t *SimpleTable) Create(bpm *bufferpool.BufferPoolManager) error {
	tree, err := btree.CreateBTree(bpm)
	if err != nil {
		return err
	}
	t.MetaPageId = tree.MetaPageId
	return nil
}

// テーブルに行を挿入する
// プライマリキーを key, 他のカラム値を value としたペアを作成し、B+Tree に挿入する
func (t *SimpleTable) Insert(bpm *bufferpool.BufferPoolManager, primaryKey []uint8, record [][]byte) error {
	btree := btree.NewBTree(t.MetaPageId)

	// キーをエンコード
	var encodedKey []byte
	Encode(record[:t.PrimaryKeyCount], &encodedKey)

	// 値をエンコード
	var encodedValue []byte
	Encode(record[t.PrimaryKeyCount:], &encodedValue)

	// B+Tree に挿入
	return btree.Insert(bpm, node.Pair{
		Key:   encodedKey,
		Value: encodedValue,
	})
}
