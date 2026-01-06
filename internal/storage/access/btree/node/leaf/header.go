package leaf

import "minesql/internal/storage/disk"

const leafHeaderSize = 16

// B+Tree のリーフノードのヘッダー情報
// PrevPageId: 8 bytes (0-7)
// NextPageId: 8 bytes (8-15)
type LeafHeader struct {
	PrevPageId disk.PageId
	NextPageId disk.PageId
}
