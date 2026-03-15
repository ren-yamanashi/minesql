package bufferpool

import (
	"minesql/internal/storage/page"
)

// BufferId は、バッファプール内のバッファページを識別するための ID (index)
type BufferId uint64

// pageTable は、PageId と BufferId の対応関係を管理するテーブル
// PageId に対応する BufferId を格納することで、該当のページがバッファプールのどの位置に格納されているかを特定できる
// - key: PageId (ページID)
// - value: BufferId (バッファID)
type pageTable map[page.PageId]BufferId
