package catalog

import (
	"github.com/ren-yamanashi/minesql/internal/storage/btree"
	"github.com/ren-yamanashi/minesql/internal/storage/buffer"
	"github.com/ren-yamanashi/minesql/internal/storage/page"
)

type UserMeta struct {
	metaPageId page.PageId
	tree       *btree.Btree // ユーザーメタデータが格納される B+Tree
}

// NewUserMeta は既存のユーザーメタデータを開く
func NewUserMeta(bp *buffer.BufferPool, metaPageId page.PageId) *UserMeta {
	tree := btree.NewBtree(bp, metaPageId)
	return &UserMeta{metaPageId: metaPageId, tree: tree}
}

func CreateUserMeta(bp *buffer.BufferPool) (*UserMeta, error) {
	tree, err := btree.CreateBtree(bp, catalogFileId)
	if err != nil {
		return nil, err
	}
	return &UserMeta{metaPageId: tree.MetaPageId, tree: tree}, nil
}

// Search は指定した検索モードでメタデータを検索し、イテレータを返す
func (um *UserMeta) Search(mode SearchMode) (*UserIterator, error) {
	iter, err := um.tree.Search(mode.encode())
	if err != nil {
		return nil, err
	}
	return newUserIterator(iter), nil
}

// Insert はレコードを挿入する
//   - username: ユーザー名
//   - host: ホスト名
//   - authString: 認証文字列
func (um *UserMeta) Insert(
	username string,
	host string,
	authString []byte,
) error {
	record := NewUserRecord(username, host, authString)
	return um.tree.Insert(record.encode())
}
