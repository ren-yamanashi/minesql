package lock

import (
	"github.com/ren-yamanashi/minesql/internal/storage/page"
)

// RowKey は行ロックの識別子
type RowKey struct {
	MetaPageId page.Id // 対象インデックスのメタページ PageId
	Key        []byte  // そのインデックスにおける行のキー (memcomparable エンコード済み)
}

// rowLockKey は Manager 内部で使う行ロック識別子
type rowLockKey struct {
	metaPageId page.Id // 対象インデックスのメタページ PageId
	keyStr     string  // 行のキー (string([]byte) で保持。[]byte は map のキーに使えないため)
}

func newRowLockKey(rk RowKey) rowLockKey {
	return rowLockKey{
		metaPageId: rk.MetaPageId,
		keyStr:     string(rk.Key),
	}
}
