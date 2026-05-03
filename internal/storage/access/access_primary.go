package access

import (
	"errors"

	"github.com/ren-yamanashi/minesql/internal/storage/btree"
	"github.com/ren-yamanashi/minesql/internal/storage/buffer"
	"github.com/ren-yamanashi/minesql/internal/storage/page"
)

var (
	ErrAlreadyDeleted = errors.New("already deleted")
)

// PrimaryIndex はプライマリインデックスへのアクセスを提供する
type PrimaryIndex struct {
	tree    *btree.Btree // プライマリインデックスの B+Tree
	pkCount int          // プライマリキーのカラム数
}

// NewPrimaryIndex は既存のプライマリインデックスを開く
func NewPrimaryIndex(bp *buffer.BufferPool, metaPageId page.PageId, pkCount int) *PrimaryIndex {
	tree := btree.NewBtree(bp, metaPageId)
	return &PrimaryIndex{
		tree:    tree,
		pkCount: pkCount,
	}
}

// CreatePrimaryIndex は空のプライマリインデックスを作成する
//   - fileId: プライマリインデックスを格納するファイルの ID
func CreatePrimaryIndex(bp *buffer.BufferPool, fileId page.FileId) (*PrimaryIndex, error) {
	tree, err := btree.CreateBtree(bp, fileId)
	if err != nil {
		return nil, err
	}
	return &PrimaryIndex{
		tree:    tree,
		pkCount: 0,
	}, nil
}

// SetPkCount はプライマリキーのカラム数を設定する
func (pi *PrimaryIndex) SetPkCount(pkCount int) {
	pi.pkCount = pkCount
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
// (論理削除済みの同一キーが存在する場合は上書きする)
func (pi *PrimaryIndex) Insert(data [][]byte) error {
	// TODO: Undo ログの記録
	record := newPrimaryRecord(pi.pkCount, 0, data)
	return pi.insert(record)
}

// SoftDelete は行を論理削除する
func (pi *PrimaryIndex) SoftDelete(data [][]byte) error {
	// TODO: Undo ログの記録
	record := newPrimaryRecord(pi.pkCount, 1, data)
	return pi.softDelete(record)
}

// UpdateInplace は行を更新する
func (pi *PrimaryIndex) UpdateInplace(old [][]byte, new [][]byte) error {
	// TODO: Undo ログの記録
	recordOld := newPrimaryRecord(pi.pkCount, 0, old)
	recordNew := newPrimaryRecord(pi.pkCount, 0, new)
	return pi.updateInplace(recordOld, recordNew)
}

// LeafPageCount はリーフページ数を取得する
func (pi *PrimaryIndex) LeafPageCount() (uint64, error) {
	return pi.tree.LeafPageCount()
}

// Height はツリーの高さを取得する
func (pi *PrimaryIndex) Height() (uint64, error) {
	return pi.tree.Height()
}

// insert は Undo ログを記録せずテーブルに行を挿入する
func (pi *PrimaryIndex) insert(pr *PrimaryRecord) error {
	// TODO: ロック処理
	record := pr.encode()
	err := pi.tree.Insert(record)
	if err == nil {
		return nil
	}
	if !errors.Is(err, btree.ErrDuplicateKey) {
		return err
	}

	// 重複キーエラーの場合、既存のレコードが論理削除済みか確認
	existing, _, err := pi.tree.FindByKey(record.Key())
	if err != nil {
		return err
	}

	deleteMark := existing.Header()[0]
	if deleteMark != 1 {
		return btree.ErrDuplicateKey
	}

	// 論理削除済みの場合は上書き
	return pi.tree.Update(record)
}

// softDelete は Undo ログを記録せず行を論理削除する
func (pi *PrimaryIndex) softDelete(pr *PrimaryRecord) error {
	// TODO: ロック処理
	record := pr.encode()
	existing, _, err := pi.tree.FindByKey(record.Key())
	if err != nil {
		return err
	}

	deleteMark := existing.Header()[0]
	if deleteMark == 1 {
		return ErrAlreadyDeleted
	}

	return pi.tree.Update(record)
}

// delete は Undo ログを記録せず行を削除する
func (pi *PrimaryIndex) delete(pr *PrimaryRecord) error {
	// TODO: ロック処理
	key := pr.encode().Key()
	_, _, err := pi.tree.FindByKey(key)
	if err != nil {
		return err
	}
	return pi.tree.Delete(key)
}

// updateInplace は Undo ログを記録せず行を更新する
func (pi *PrimaryIndex) updateInplace(old *PrimaryRecord, new *PrimaryRecord) error {
	// TODO: ロック処理
	_, _, err := pi.tree.FindByKey(old.encode().Key())
	if err != nil {
		return err
	}
	return pi.tree.Update(new.encode())
}
