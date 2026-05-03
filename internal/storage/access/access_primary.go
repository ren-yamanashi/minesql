package access

import (
	"errors"

	"github.com/ren-yamanashi/minesql/internal/storage/btree"
	"github.com/ren-yamanashi/minesql/internal/storage/buffer"
	"github.com/ren-yamanashi/minesql/internal/storage/page"
)

// PrimaryIndex はプライマリインデックスへのアクセスを提供する
type PrimaryIndex struct {
	bufferPool *buffer.BufferPool
	tree       *btree.Btree // プライマリインデックスの B+Tree
	TableName  string
	PkCount    int // プライマリキーのカラム数 (先頭から連続している想定)
}

// NewPrimaryIndex は既存のプライマリインデックスを開く
func NewPrimaryIndex(bp *buffer.BufferPool, metaPageId page.PageId, table string, pkCount int) *PrimaryIndex {
	tree := btree.NewBtree(bp, metaPageId)
	return &PrimaryIndex{
		TableName: table,
		tree:      tree,
		PkCount:   pkCount,
	}
}

// Create は空のプライマリインデックスを作成する
func CreatePrimaryIndex() *PrimaryIndex {
	tree := btree.CreateBtree()
}

// Search は指定した検索モードでテーブルを検索し、PrimaryIterator を返す
func (pi *PrimaryIndex) Search(mode SearchMode) (*PrimaryIterator, error) {
	iter, err := pi.tree.Search(mode.encode())
	if err != nil {
		return nil, err
	}
	return newPrimaryIterator(iter), nil
}

// Insert は行を挿入する
// (論理削除済みの同一キーが存在する場合は Update で上書きする)
func (pi *PrimaryIndex) Insert(data [][]byte) error {
	// TODO: Undo ログの記録
	return pi.insert(data)
}

// SoftDelete は行を論理削除する
func (pi *PrimaryIndex) SoftDelete(data [][]byte) error {
	// TODO: Undo ログの記録
	return pi.softDelete(data)
}

// UpdateInplace は行を更新する
func (pi *PrimaryIndex) UpdateInplace(old [][]byte, new [][]byte) error {
	// TODO: Undo ログの記録
	return pi.updateInplace(old, new)
}

// LeafPageCount はリーフページ数を取得する
func (pi *PrimaryIndex) LeafPageCount() (uint64, error) {
	return pi.tree.LeafPageCount()
}

// Height はツリーの高さを取得する
func (pi *PrimaryIndex) Height(bp *buffer.BufferPool) (uint64, error) {
	return pi.tree.Height()
}

// insert は Undo ログを記録せずテーブルに行を挿入する
func (pi *PrimaryIndex) insert(data [][]byte) error {
	// TODO: ロック処理
	record := newPrimaryRecord(pi.PkCount, 0, data).encode()
	err := pi.tree.Insert(record)

	// 重複キーエラーの場合、既存のレコードが論理削除済みか確認
	if err != nil && errors.Is(err, btree.ErrDuplicateKey) {
		existing, _, err := pi.tree.FindByKey(record.Key())
		if err != nil {
			return err
		}
		if existing.Header()[0] != 1 {
			return btree.ErrDuplicateKey
		}

		// 論理削除済みの場合は上書き
		err = pi.tree.Update(record)
		if err != nil {
			return err
		}
	}
	return err
}

// softDelete は Undo ログを記録せず行を論理削除する
func (pi *PrimaryIndex) softDelete(data [][]byte) error {
	// TODO: ロック処理
	record := newPrimaryRecord(pi.PkCount, 1, data).encode()
	_, _, err := pi.tree.FindByKey(record.Key())
	if err != nil {
		return err
	}
	return pi.tree.Update(record)
}

// delete は Undo ログを記録せず行を削除する
func (pi *PrimaryIndex) delete(data [][]byte) error {
	// TODO: ロック処理
	key := newPrimaryRecord(pi.PkCount, 0, data).encode().Key()
	_, _, err := pi.tree.FindByKey(key)
	if err != nil {
		return err
	}
	return pi.tree.Delete(key)
}

// updateInplace は Undo ログを記録せず行を更新する
func (pi *PrimaryIndex) updateInplace(old [][]byte, new [][]byte) error {
	// TODO: ロック処理
	recordOld := newPrimaryRecord(pi.PkCount, 0, old).encode()
	recordNew := newPrimaryRecord(pi.PkCount, 0, new).encode()

	_, _, err := pi.tree.FindByKey(recordOld.Key())
	if err != nil {
		return err
	}
	return pi.tree.Update(recordNew)
}
