package list

import (
	"github.com/ren-yamanashi/minesql/internal/storage/buffer"
	"github.com/ren-yamanashi/minesql/internal/storage/page"
)

// BranchList はブランチノード群の操作を行う
type BranchList struct {
	bufferPool *buffer.BufferPool
	fileId     page.FileId
}

func NewBranchList(bufferPool *buffer.BufferPool, fileId page.FileId) *BranchList {
	return &BranchList{
		bufferPool: bufferPool,
		fileId:     fileId,
	}
}
