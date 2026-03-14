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

// テーブルから行を削除する
func (t *Table) Delete(bpm *bufferpool.BufferPoolManager, record [][]byte) error {
	btree := btree.NewBTree(t.MetaPageId)

	// キーをエンコード
	var encodedKey []byte
	Encode(record[:t.PrimaryKeyCount], &encodedKey)

	// B+Tree から削除
	err := btree.Delete(bpm, encodedKey)
	if err != nil {
		return err
	}

	// ユニークインデックスから削除
	for _, ui := range t.UniqueIndexes {
		err := ui.Delete(bpm, record)
		if err != nil {
			return err
		}
	}

	return nil
}

// テーブルから行を更新する
func (t *Table) Update(bpm *bufferpool.BufferPoolManager, oldRecord [][]byte, newRecord [][]byte) error {
	btree := btree.NewBTree(t.MetaPageId)

	// キーをエンコード
	var encodedOldKey []byte
	Encode(oldRecord[:t.PrimaryKeyCount], &encodedOldKey)
	var encodedNewKey []byte
	Encode(newRecord[:t.PrimaryKeyCount], &encodedNewKey)

	// 値をエンコード
	var encodedNewValue []byte
	Encode(newRecord[t.PrimaryKeyCount:], &encodedNewValue)

	// キーが一致しない場合は、B+Tree から古いキーに該当するペアを削除し、新しいキーに該当するペアを挿入する
	if string(encodedOldKey) != string(encodedNewKey) {
		err := btree.Delete(bpm, encodedOldKey)
		if err != nil {
			return err
		}
		err = btree.Insert(bpm, node.NewPair(encodedNewKey, encodedNewValue))
		if err != nil {
			return err
		}
	} else {
		// キーが一致する場合は、B+Tree のペアを更新する
		err := btree.Update(bpm, node.NewPair(encodedOldKey, encodedNewValue))
		if err != nil {
			return err
		}
	}

	// ユニークインデックスを更新
	for _, ui := range t.UniqueIndexes {
		err := ui.Update(bpm, oldRecord, newRecord, encodedNewKey)
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
