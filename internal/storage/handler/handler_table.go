package handler

import (
	"minesql/internal/storage/access"
	"minesql/internal/storage/buffer"
	"minesql/internal/storage/encode"
)

// TableHandler はテーブルへの操作を提供する
type TableHandler struct {
	inner *access.TableAccessMethod
}

// NewTableHandler は access.TableAccessMethod をラップした TableHandler を生成する
func NewTableHandler(t *access.TableAccessMethod) *TableHandler {
	return &TableHandler{inner: t}
}

func (t *TableHandler) Insert(bp *buffer.BufferPool, columns [][]byte) error {
	return t.inner.Insert(bp, columns)
}

func (t *TableHandler) Delete(bp *buffer.BufferPool, columns [][]byte) error {
	return t.inner.Delete(bp, columns)
}

func (t *TableHandler) SoftDelete(bp *buffer.BufferPool, columns [][]byte) error {
	return t.inner.SoftDelete(bp, columns)
}

func (t *TableHandler) UpdateInplace(bp *buffer.BufferPool, oldColumns, newColumns [][]byte) error {
	return t.inner.UpdateInplace(bp, oldColumns, newColumns)
}

func (t *TableHandler) Search(bp *buffer.BufferPool, mode SearchMode) (TableIterator, error) {
	accessMode := toAccessSearchMode(mode)
	return t.inner.Search(bp, accessMode)
}

func (t *TableHandler) GetUniqueIndexByName(name string) (*IndexHandler, error) {
	ui, err := t.inner.GetUniqueIndexByName(name)
	if err != nil {
		return nil, err
	}
	return &IndexHandler{inner: ui, tableInner: t.inner}, nil
}

func (t *TableHandler) PrimaryKeyCount() uint8 {
	return t.inner.PrimaryKeyCount
}

func (t *TableHandler) EncodeKey(columns [][]byte) []byte {
	var encoded []byte
	encode.Encode(columns[:t.inner.PrimaryKeyCount], &encoded)
	return encoded
}

func (t *TableHandler) Create(bp *buffer.BufferPool) error {
	return t.inner.Create(bp)
}
