package access

import (
	"fmt"
	"minesql/internal/storage/btree"
	"minesql/internal/storage/bufferpool"
	"minesql/internal/storage/memcomparable"
	"minesql/internal/storage/page"
)

// TableAccessMethod はテーブルへのアクセスを提供する
//
// 1 つの AccessMethod は 1 つの *.db (= 1 テーブル) ファイルに対応する
type TableAccessMethod struct {
	Name            string                     // テーブル名
	MetaPageId      page.PageId                // テーブルの内容が入っている B+Tree のメタページの ID
	PrimaryKeyCount uint8                      // プライマリキーの列数 (プライマリキーは先頭から連続している想定) (例: プライマリキーが (id, name) の場合、PrimaryKeyCount は 2 になる)
	UniqueIndexes   []*UniqueIndexAccessMethod // テーブルに紐づくユニークインデックス群
}

func NewTableAccessMethod(name string, metaPageId page.PageId, primaryKeyCount uint8, uniqueIndexes []*UniqueIndexAccessMethod) TableAccessMethod {
	return TableAccessMethod{
		Name:            name,
		MetaPageId:      metaPageId,
		PrimaryKeyCount: primaryKeyCount,
		UniqueIndexes:   uniqueIndexes,
	}
}

// Create は空のテーブルを新規作成する
func (t *TableAccessMethod) Create(bp *bufferpool.BufferPool) error {
	// テーブルの B+Tree を作成
	tree, err := btree.CreateBPlusTree(bp, t.MetaPageId)
	if err != nil {
		return err
	}
	t.MetaPageId = tree.MetaPageId

	// ユニークインデックスを作成
	for _, ui := range t.UniqueIndexes {
		err = ui.Create(bp)
		if err != nil {
			return err
		}
	}
	return nil
}

// Insert はテーブルに行を挿入する
//
// プライマリキーを key, 他のカラム値を value としたペアを作成し、B+Tree に挿入する
func (t *TableAccessMethod) Insert(bp *bufferpool.BufferPool, record [][]byte) error {
	btr := btree.NewBPlusTree(t.MetaPageId)

	// キーをエンコード
	var encodedKey []byte
	memcomparable.Encode(record[:t.PrimaryKeyCount], &encodedKey)

	// 値をエンコード
	var encodedValue []byte
	memcomparable.Encode(record[t.PrimaryKeyCount:], &encodedValue)

	// B+Tree に挿入
	err := btr.Insert(bp, btree.NewPair(encodedKey, encodedValue))
	if err != nil {
		return err
	}

	// ユニークインデックスに挿入
	for _, ui := range t.UniqueIndexes {
		err := ui.Insert(bp, encodedKey, record)
		if err != nil {
			return err
		}
	}

	return nil
}

// Delete はテーブルから行を削除する
func (t *TableAccessMethod) Delete(bp *bufferpool.BufferPool, record [][]byte) error {
	btr := btree.NewBPlusTree(t.MetaPageId)

	// キーをエンコード
	var encodedKey []byte
	memcomparable.Encode(record[:t.PrimaryKeyCount], &encodedKey)

	// B+Tree から削除
	err := btr.Delete(bp, encodedKey)
	if err != nil {
		return err
	}

	// ユニークインデックスから削除
	for _, ui := range t.UniqueIndexes {
		err := ui.Delete(bp, record)
		if err != nil {
			return err
		}
	}

	return nil
}

// Update はテーブルから行を更新する
//
// oldRecord: 更新前の行 (プライマリキーとその他のカラム値を含む)
//
// newRecord: 更新後の行 (プライマリキーとその他のカラム値を含む)
func (t *TableAccessMethod) Update(bp *bufferpool.BufferPool, oldRecord [][]byte, newRecord [][]byte) error {
	btr := btree.NewBPlusTree(t.MetaPageId)

	// キーをエンコード
	var encodedOldKey []byte
	memcomparable.Encode(oldRecord[:t.PrimaryKeyCount], &encodedOldKey)
	var encodedNewKey []byte
	memcomparable.Encode(newRecord[:t.PrimaryKeyCount], &encodedNewKey)

	// 値をエンコード
	var encodedNewValue []byte
	memcomparable.Encode(newRecord[t.PrimaryKeyCount:], &encodedNewValue)

	// キーが一致しない場合は、B+Tree から古いキーに該当するペアを削除し、新しいキーに該当するペアを挿入する
	if string(encodedOldKey) != string(encodedNewKey) {
		err := btr.Delete(bp, encodedOldKey)
		if err != nil {
			return err
		}
		err = btr.Insert(bp, btree.NewPair(encodedNewKey, encodedNewValue))
		if err != nil {
			return err
		}
	} else {
		// キーが一致する場合は、B+Tree のペアを更新する
		err := btr.Update(bp, btree.NewPair(encodedOldKey, encodedNewValue))
		if err != nil {
			return err
		}
	}

	// ユニークインデックスを更新
	for _, ui := range t.UniqueIndexes {
		err := ui.Update(bp, oldRecord, newRecord, encodedNewKey)
		if err != nil {
			return err
		}
	}

	return nil
}

// Search は指定した検索モードでテーブルを検索し、ClusteredIndexIterator を返す
func (t *TableAccessMethod) Search(bp *bufferpool.BufferPool, mode RecordSearchMode) (*ClusteredIndexIterator, error) {
	btr := btree.NewBPlusTree(t.MetaPageId)
	iterator, err := btr.Search(bp, mode.encode())
	if err != nil {
		return nil, err
	}
	return newClusteredIndexIterator(iterator, bp), nil
}

// GetUniqueIndexByName はインデックス名からユニークインデックスを取得する
func (t *TableAccessMethod) GetUniqueIndexByName(indexName string) (*UniqueIndexAccessMethod, error) {
	for _, ui := range t.UniqueIndexes {
		if ui.Name == indexName {
			return ui, nil
		}
	}
	return nil, fmt.Errorf("unique index %s not found in table %s", indexName, t.Name)
}

// LeafPageCount は B+Tree のメタページからリーフページ数を取得する
func (t *TableAccessMethod) LeafPageCount(bp *bufferpool.BufferPool) (uint64, error) {
	btr := btree.NewBPlusTree(t.MetaPageId)
	return btr.LeafPageCount(bp)
}
