package undo

import (
	"errors"

	"github.com/ren-yamanashi/minesql/internal/storage/lock"
	"github.com/ren-yamanashi/minesql/internal/storage/page"
)

type (
	UndoNumber = uint32
	RecordType int
)

const (
	headerTrxIdOffset      = 0
	headerUndoNumOffset    = 4
	headerRecordTypeOffset = 8
	headerDataLenOffset    = 9
	recordHeaderSize       = 11 // TrxId(4) + UndoNum(4) + Type(1) + DataLen(2)
)

const (
	recordTypeUnknown RecordType = iota
	RecordTypeInsert
	RecordTypeDelete
	RecordTypeUpdate
)

var ErrInvalidRecord = errors.New("undo: invalid record")

var (
	_ Record = (*UpdateRecord)(nil)
	_ Record = (*InsertRecord)(nil)
	_ Record = (*DeleteRecord)(nil)
)

type Record interface {
	TableFileId() page.FileId
	Serialize(trxId lock.TrxId, undoNum UndoNumber) []byte
}
