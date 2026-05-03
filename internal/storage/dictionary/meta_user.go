package dictionary

import (
	"github.com/ren-yamanashi/minesql/internal/storage/btree"
	"github.com/ren-yamanashi/minesql/internal/storage/buffer"
	"github.com/ren-yamanashi/minesql/internal/storage/page"
)

type UserMeta struct {
	tree *btree.Btree // ユーザーメタデータが格納される B+Tree
}

func NewUserMeta(bp *buffer.BufferPool, metaPageId page.PageId) *UserMeta {
	tree := btree.NewBtree(bp, metaPageId)
	return &UserMeta{tree: tree}
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
	record := newUserRecord(username, host, authString)
	return um.tree.Insert(record.encode())
}
