package undo

import (
	"encoding/binary"
	"errors"

	"github.com/ren-yamanashi/minesql/internal/storage/page"
)

const allocateFileIdSize = 4

// ErrInvalidAllocateFileIdUndoRecord は AllocateFileIdUndoRecord のデシリアライズに失敗したことを表す
var ErrInvalidAllocateFileIdUndoRecord = errors.New("undo: invalid allocate file id undo record")

// AllocateFileIdUndoRecord は物理ファイル確保 (FileId 採番) を取り消すための DDL Undo レコード
//   - fileId: 確保した FileId。 Rollback 時は対応する物理ファイルを削除する
type AllocateFileIdUndoRecord struct {
	fileId page.FileId
}

func NewAllocateFileIdUndoRecord(fileId page.FileId) AllocateFileIdUndoRecord {
	return AllocateFileIdUndoRecord{fileId: fileId}
}

func (r AllocateFileIdUndoRecord) FileId() page.FileId { return r.fileId }

// Serialize は AllocateFileIdUndoRecord をバイト列にエンコードする
//   - return: FileId のバイト列 (4 バイト)
func (r AllocateFileIdUndoRecord) Serialize() []byte {
	buf := make([]byte, allocateFileIdSize)
	binary.BigEndian.PutUint32(buf, uint32(r.fileId))
	return buf
}

// DeserializeAllocateFileIdUndoRecord はバイト列から AllocateFileIdUndoRecord を復元する
func DeserializeAllocateFileIdUndoRecord(data []byte) (AllocateFileIdUndoRecord, error) {
	if len(data) != allocateFileIdSize {
		return AllocateFileIdUndoRecord{}, ErrInvalidAllocateFileIdUndoRecord
	}
	fileId := page.FileId(binary.BigEndian.Uint32(data))
	return AllocateFileIdUndoRecord{fileId: fileId}, nil
}
