package bufferpool

import (
	"minesql/internal/storage/disk"
)

type PageTable map[disk.PageId]BufferId
