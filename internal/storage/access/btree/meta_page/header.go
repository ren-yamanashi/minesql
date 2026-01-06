package metapage

import "minesql/internal/storage/disk"

const headerSize = 8

type Header struct {
	RootPageId disk.PageId
}
