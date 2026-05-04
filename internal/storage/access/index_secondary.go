package access

import (
	"bytes"
	"errors"

	"github.com/ren-yamanashi/minesql/internal/storage/btree"
	"github.com/ren-yamanashi/minesql/internal/storage/buffer"
	"github.com/ren-yamanashi/minesql/internal/storage/catalog"
	"github.com/ren-yamanashi/minesql/internal/storage/page"
)

// SecondaryIndex はセカンダリインデックスへのアクセスを提供する
type SecondaryIndex struct {
	catalog     *catalog.Catalog
	tree        *btree.Btree    // セカンダリインデックスの B+Tree
	primaryTree *btree.Btree    // プライマリインデックスの B+Tree
	fileId      page.FileId     // インデックスが属するテーブルの FileId
	indexId     catalog.IndexId // インデックス ID
	indexName   string          // インデックス名
	unique      bool            // ユニーク制約の有無
}

type NewSecondaryIndexInput struct {
	MetaPageId  page.PageId     // セカンダリインデックスの MetaPageId
	PrimaryTree *btree.Btree    // プライマリインデックスの B+Tree
	IndexId     catalog.IndexId // インデックス ID
	IndexName   string          // インデックス名
	Unique      bool            // ユニークインデックスか
}

// NewSecondaryIndex は既存のセカンダリインデックスを開く
func NewSecondaryIndex(
	ct *catalog.Catalog,
	bp *buffer.BufferPool,
	input NewSecondaryIndexInput,
) *SecondaryIndex {
	tree := btree.NewBtree(bp, input.MetaPageId)
	return &SecondaryIndex{
		catalog:     ct,
		tree:        tree,
		primaryTree: input.PrimaryTree,
		fileId:      input.PrimaryTree.MetaPageId.FileId,
		indexId:     input.IndexId,
		indexName:   input.IndexName,
		unique:      input.Unique,
	}
}

type CreateSecondaryIndexInput struct {
	FileId      page.FileId     // インデックスが属するテーブルの FileId
	PrimaryTree *btree.Btree    // プライマリインデックスの B+Tree
	IndexId     catalog.IndexId // インデックス ID
	IndexName   string          // インデックス名
	Unique      bool            // ユニークか
}

// CreateSecondaryIndex は空のセカンダリインデックスを作成する
func CreateSecondaryIndex(
	ct *catalog.Catalog,
	bp *buffer.BufferPool,
	input CreateSecondaryIndexInput,
) (*SecondaryIndex, error) {
	tree, err := btree.CreateBtree(bp, input.FileId)
	if err != nil {
		return nil, err
	}
	return &SecondaryIndex{
		catalog:     ct,
		tree:        tree,
		primaryTree: input.PrimaryTree,
		fileId:      input.PrimaryTree.MetaPageId.FileId,
		indexId:     input.IndexId,
		indexName:   input.IndexName,
		unique:      input.Unique,
	}, nil
}

// Search は指定した検索モードでインデックスを検索し、イテレータを返す
func (si *SecondaryIndex) Search(mode SearchMode) (*SecondaryIterator, error) {
	iter, err := si.tree.Search(mode.encode())
	if err != nil {
		return nil, err
	}
	return newSecondaryIterator(si.indexName, iter, si.catalog, si.primaryTree), nil
}

// Insert は行を挿入する
//   - unique index の場合かつセカンダリキーの重複があるとエラー
//   - 論理削除済みの同一キー (SK + PK) が存在する場合は上書きする
func (si *SecondaryIndex) Insert(colNames, values, pk []string) error {
	record, err := newSecondaryRecord(si.catalog, newSecondaryRecordInput{
		fileId:     si.fileId,
		deleteMark: 0,
		indexName:  si.indexName,
		colNames:   colNames,
		values:     values,
		pk:         pk,
	})
	if err != nil {
		return err
	}
	if si.unique {
		if err := si.checkUnique(record); err != nil {
			return err
		}
	}
	return si.insert(record)
}

// Delete は行を物理削除する
func (si *SecondaryIndex) Delete(record *SecondaryRecord) error {
	// TODO: ロック処理
	return si.tree.Delete(record.encode().Key())
}

// SoftDelete は行を論理削除する
func (si *SecondaryIndex) SoftDelete(record *SecondaryRecord) error {
	// TODO: ロック処理
	// deleteMark を 1 にしたレコードで上書き
	deleted, err := newSecondaryRecord(si.catalog, newSecondaryRecordInput{
		fileId:     si.fileId,
		deleteMark: 1,
		indexName:  si.indexName,
		colNames:   record.ColNames,
		values:     record.Values,
		pk:         record.Pk,
	})
	if err != nil {
		return err
	}
	return si.tree.Update(deleted.encode())
}

// LeafPageCount はリーフページ数を取得する
func (si *SecondaryIndex) LeafPageCount() (uint64, error) {
	return si.tree.LeafPageCount()
}

// Height はツリーの高さを取得する
func (si *SecondaryIndex) Height() (uint64, error) {
	return si.tree.Height()
}

// insert は行を挿入する
func (si *SecondaryIndex) insert(sr *SecondaryRecord) error {
	// TODO: ロック処理
	record := sr.encode()
	err := si.tree.Insert(record)
	if err == nil {
		return nil
	}
	if !errors.Is(err, btree.ErrDuplicateKey) {
		return err
	}

	// 重複キーエラーの場合、既存のレコードが論理削除済みか確認
	existing, _, err := si.tree.FindByKey(record.Key())
	if err != nil {
		return err
	}

	deleteMark := existing.Header()[0]
	if deleteMark != 1 {
		return btree.ErrDuplicateKey
	}

	// 論理削除済みの場合は上書き
	return si.tree.Update(record)
}

// checkUnique は record のセカンダリキーに対して active なレコードが存在するか確認する
//   - return: 存在する場合は ErrDuplicateKey
func (si *SecondaryIndex) checkUnique(sr *SecondaryRecord) error {
	encodedSk := sr.encodedSecondaryKey()
	// セカンダリインデックスのキーは SK+PK の構成であり、SK のみで SearchModeKey を使うと SK 以上の最初のキーの位置に着地する
	iter, err := si.tree.Search(btree.SearchModeKey{Key: encodedSk})
	if err != nil {
		return err
	}

	for {
		existing, ok, err := iter.Get()
		if err != nil {
			return err
		}
		if !ok {
			return nil
		}

		// キー同士の比較
		if len(existing.Key()) < len(encodedSk) {
			return nil
		}
		existingSk := existing.Key()[:len(encodedSk)]
		if !bytes.Equal(existingSk, encodedSk) {
			return nil
		}

		deleteMark := existing.Header()[0]
		// 論理削除済みでない場合は重複
		if deleteMark != 1 {
			return btree.ErrDuplicateKey
		}
		// 論理削除済みの場合は次のレコードへ
		if err := iter.Advance(); err != nil {
			return err
		}
	}
}
