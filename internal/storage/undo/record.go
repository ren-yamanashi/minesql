package undo

import (
	"errors"

	"github.com/ren-yamanashi/minesql/internal/storage/lock"
)

type (
	UndoNumber = uint32
	RecordType int
)

const (
	headerTrxIdOffset                 = 0
	headerUndoNumOffset               = 4
	headerRecordTypeOffset            = 8
	headerDataLenOffset               = 9
	recordHeaderSize                  = 11 // TrxId(4) + UndoNum(4) + Type(1) + DataLen(2)
	Insert                 RecordType = iota + 1
	Delete
	Update
)

var ErrInvalid = errors.New("invalid undo record")

type Record interface {
	Serialize(trxId lock.TrxId, undoNum UndoNumber) []byte
}
