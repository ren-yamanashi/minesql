package undo

import (
	"github.com/ren-yamanashi/minesql/internal/storage/btree"
	"github.com/ren-yamanashi/minesql/internal/storage/lock"
	"github.com/ren-yamanashi/minesql/internal/storage/page"
)

type UpdateRecord struct {
	tableFileId   page.FileId  // テーブルの FileId
	prevRecord    btree.Record // 更新前のレコード
	newRecord     btree.Record // 更新後のレコード
	prevLastTrxId lock.TrxId
	prevRollPtr   Pointer
}

func NewUpdateRecord(
	tableFileId page.FileId,
	prevRecord, newRecord btree.Record,
	prevLastTrxId lock.TrxId,
	prevRollPtr Pointer,
) UpdateRecord {
	return UpdateRecord{
		tableFileId:   tableFileId,
		prevRecord:    prevRecord,
		newRecord:     newRecord,
		prevLastTrxId: prevLastTrxId,
		prevRollPtr:   prevRollPtr,
	}
}

func (ur UpdateRecord) TableFileId() page.FileId { return ur.tableFileId }
func (ur UpdateRecord) PrevRecord() btree.Record { return ur.prevRecord }
func (ur UpdateRecord) NewRecord() btree.Record  { return ur.newRecord }

func (ur UpdateRecord) Serialize(trxId lock.TrxId, undoNum UndoNumber) []byte {
	fields := Fields{
		trxId:         trxId,
		undoNum:       undoNum,
		recordType:    RecordTypeUpdate,
		prevLastTrxId: ur.prevLastTrxId,
		prevRollPtr:   ur.prevRollPtr,
		tableFileId:   ur.tableFileId,
		columnSets:    [][][]byte{ur.prevRecord, ur.newRecord},
	}
	return fields.serialize()
}
