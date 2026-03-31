package access

import (
	"errors"
	"fmt"
	"minesql/internal/storage/btree"
	"minesql/internal/storage/btree/node"
	"minesql/internal/storage/buffer"
	"minesql/internal/storage/encode"
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

// Search は指定した検索モードでテーブルを検索し、ClusteredIndexIterator を返す
func (t *TableAccessMethod) Search(bp *buffer.BufferPool, mode RecordSearchMode) (*TableIterator, error) {
	btr := btree.NewBTree(t.MetaPageId)
	iterator, err := btr.Search(bp, mode.encode())
	if err != nil {
		return nil, err
	}
	return newTableIterator(iterator, bp), nil
}

// Create は空のテーブルを新規作成する
func (t *TableAccessMethod) Create(bp *buffer.BufferPool) error {
	// テーブルの B+Tree を作成
	tree, err := btree.CreateBTree(bp, t.MetaPageId)
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
// ソフトデリート済みの同一キーが存在する場合は Update で上書きする
func (t *TableAccessMethod) Insert(bp *buffer.BufferPool, columns [][]byte) error {
	btr := btree.NewBTree(t.MetaPageId)

	btrRecord := t.encodeBTreeRecord(columns, 0)
	encodedKey := t.encodeKey(columns)

	err := btr.Insert(bp, btrRecord)
	if err != nil {
		if !errors.Is(err, btree.ErrDuplicateKey) {
			return err
		}

		// 重複キーの場合、既存レコードがソフトデリート済みか確認する
		existing, findErr := btr.FindByKey(bp, encodedKey)
		if findErr != nil {
			return findErr
		}
		if existing.HeaderBytes()[0] != 1 {
			return btree.ErrDuplicateKey
		}

		// ソフトデリート済みなので Update で上書き (DeleteMark は 0 に戻る)
		err = btr.Update(bp, btrRecord)
		if err != nil {
			return err
		}
	}

	// ユニークインデックスに挿入
	for _, ui := range t.UniqueIndexes {
		err := ui.Insert(bp, encodedKey, columns)
		if err != nil {
			return err
		}
	}

	return nil
}

// Delete はテーブルから行を物理削除する
//   - columns: 削除する行のカラム値 (プライマリキーを含む全カラム)
func (t *TableAccessMethod) Delete(bp *buffer.BufferPool, columns [][]byte) error {
	btr := btree.NewBTree(t.MetaPageId)

	encodedKey := t.encodeKey(columns)
	if err := btr.Delete(bp, encodedKey); err != nil {
		return err
	}

	// ユニークインデックスを物理削除
	for _, ui := range t.UniqueIndexes {
		err := ui.Delete(bp, encodedKey, columns)
		if err != nil {
			return err
		}
	}

	return nil
}

// SoftDelete はテーブルから行をソフトデリートする
//
// B+Tree からレコードを物理削除せず、DeleteMark を 1 に設定する
func (t *TableAccessMethod) SoftDelete(bp *buffer.BufferPool, columns [][]byte) error {
	btr := btree.NewBTree(t.MetaPageId)

	btrRecord := t.encodeBTreeRecord(columns, 1)
	if err := btr.Update(bp, btrRecord); err != nil {
		return err
	}

	// ユニークインデックスをソフトデリート
	encodedKey := t.encodeKey(columns)
	for _, ui := range t.UniqueIndexes {
		err := ui.Delete(bp, encodedKey, columns)
		if err != nil {
			return err
		}
	}

	return nil
}

// UpdateInplace はテーブルの行をインプレース更新する
//
// プライマリキーが変わらないことを前提とする (プライマリキーが変わる場合は呼び出し側で SoftDelete + Insert を行う)
//
// ユニークインデックスは物理削除 (old) + 挿入 (new) で更新する
func (t *TableAccessMethod) UpdateInplace(bp *buffer.BufferPool, oldColumns [][]byte, newColumns [][]byte) error {
	btr := btree.NewBTree(t.MetaPageId)
	btrRecord := t.encodeBTreeRecord(newColumns, 0)
	if err := btr.Update(bp, btrRecord); err != nil {
		return err
	}

	// ユニークインデックスを更新 (物理削除 + Insert)
	encodedOldKey := t.encodeKey(oldColumns)
	encodedNewKey := t.encodeKey(newColumns)
	for _, ui := range t.UniqueIndexes {
		err := ui.Delete(bp, encodedOldKey, oldColumns)
		if err != nil {
			return err
		}
		err = ui.Insert(bp, encodedNewKey, newColumns)
		if err != nil {
			return err
		}
	}

	return nil
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
func (t *TableAccessMethod) LeafPageCount(bp *buffer.BufferPool) (uint64, error) {
	btr := btree.NewBTree(t.MetaPageId)
	return btr.LeafPageCount(bp)
}

// Height は B+Tree のメタページからツリーの高さを取得する
func (t *TableAccessMethod) Height(bp *buffer.BufferPool) (uint64, error) {
	btr := btree.NewBTree(t.MetaPageId)
	return btr.Height(bp)
}

// encodeKey はカラム値からプライマリキー部分を Memcomparable format でエンコードする
func (t *TableAccessMethod) encodeKey(columns [][]byte) []byte {
	var encoded []byte
	encode.Encode(columns[:t.PrimaryKeyCount], &encoded)
	return encoded
}

// encodeBTreeRecord はカラム値を B+Tree レコードに変換する
func (t *TableAccessMethod) encodeBTreeRecord(columns [][]byte, deleteMark byte) node.Record {
	var key, nonKey []byte
	encode.Encode(columns[:t.PrimaryKeyCount], &key)
	encode.Encode(columns[t.PrimaryKeyCount:], &nonKey)
	return node.NewRecord([]byte{deleteMark}, key, nonKey)
}
