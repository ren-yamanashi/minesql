package bufferpool

import (
	"minesql/internal/storage/page"
)

type pageTable map[page.PageId]BufferId
