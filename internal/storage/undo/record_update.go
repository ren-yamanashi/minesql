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

func NewUpdateRecord(tableFileId page.FileId, prevRecord, newRecord btree.Record, prevLastTrxId lock.TrxId, prevRollPtr Pointer) UpdateRecord {
	return UpdateRecord{
		tableFileId:   tableFileId,
		prevRecord:    prevRecord,
		newRecord:     newRecord,
		prevLastTrxId: prevLastTrxId,
		prevRollPtr:   prevRollPtr,
	}
}

// TableFileId はテーブルの FileId を返す
func (ur UpdateRecord) TableFileId() page.FileId {
	return ur.tableFileId
}

// PrevRecord は更新前のレコードを返す
func (ur UpdateRecord) PrevRecord() btree.Record {
	return ur.prevRecord
}

// PrevRecord は更新後のレコードを返す
func (ur UpdateRecord) NewRecord() btree.Record {
	return ur.newRecord
}

// serialize は UpdateRecord を バイト列にシリアライズする
func (ur UpdateRecord) serialize(trxId lock.TrxId, undoNum undoNumber) []byte {
	fields := Fields{
		TrxId:         trxId,
		UndoNum:       undoNum,
		RecordType:    RecordTypeUpdate,
		PrevLastTrxId: ur.prevLastTrxId,
		PrevRollPtr:   ur.prevRollPtr,
		TableFileId:   ur.tableFileId,
		ColumnSets:    [][][]byte{ur.prevRecord, ur.newRecord},
	}
	return fields.Serialize()
}
