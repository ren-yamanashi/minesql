package undo

import (
	"errors"

	"github.com/ren-yamanashi/minesql/internal/storage/lock"
	"github.com/ren-yamanashi/minesql/internal/storage/page"
)

type (
	undoNumber = uint32
	recordType int
)

const (
	headerTrxIdOffset      = 0
	headerUndoNumOffset    = 4
	headerRecordTypeOffset = 8
	headerDataLenOffset    = 9
	recordHeaderSize       = 11 // TrxId(4) + UndoNum(4) + Type(1) + DataLen(2)
)

const (
	RecordTypeInsert recordType = iota + 1
	RecordTypeDelete
	RecordTypeUpdate
)

var ErrInvalidRecord = errors.New("undo: invalid record")

type Record interface {
	TableFileId() page.FileId
	serialize(trxId lock.TrxId, undoNum undoNumber) []byte
}
