package bufferpool

import (
	"minesql/internal/storage/disk"
)

type pageTable map[disk.PageId]BufferId
