package list

import (
	"errors"

	"github.com/ren-yamanashi/minesql/internal/storage/buffer"
	"github.com/ren-yamanashi/minesql/internal/storage/page"
)

var (
	ErrDuplicateKey = errors.New("duplicate key")
	ErrKeyNotFound  = errors.New("key not found")
)

// LeafList はリーフノード群の操作を行う
type LeafList struct {
	bufferPool *buffer.BufferPool
	fileId     page.FileId
}

func NewLeafList(bufferPool *buffer.BufferPool, fileId page.FileId) *LeafList {
	return &LeafList{
		bufferPool: bufferPool,
		fileId:     fileId,
	}
}
