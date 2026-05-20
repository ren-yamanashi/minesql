package undo

import (
	"github.com/ren-yamanashi/minesql/internal/storage/btree/node"
	"github.com/ren-yamanashi/minesql/internal/storage/lock"
	"github.com/ren-yamanashi/minesql/internal/storage/page"
)

type UpdateRecord struct {
	tableFileId   page.FileId // テーブルの FileId
	PrevRecord    node.Record // 更新前のレコード
	NewRecord     node.Record // 更新後のレコード
	PrevLastTrxId lock.TrxId
	PrevRollPtr   Pointer
}

func NewUpdateRecord(tableFileId page.FileId, prevRecord, newRecord node.Record, prevLastTrxId lock.TrxId, prevRollPtr Pointer) UpdateRecord {
	return UpdateRecord{
		tableFileId:   tableFileId,
		PrevRecord:    prevRecord,
		NewRecord:     newRecord,
		PrevLastTrxId: prevLastTrxId,
		PrevRollPtr:   prevRollPtr,
	}
}

// Serialize は UpdateRecord を バイト列にシリアライズする
func (ur UpdateRecord) Serialize(trxId lock.TrxId, undoNum UndoNumber) []byte {
	fields := Fields{
		TrxId:         trxId,
		UndoNum:       undoNum,
		RecordType:    Update,
		PrevLastTrxId: ur.PrevLastTrxId,
		PrevRollPtr:   ur.PrevRollPtr,
		TableFileId:   ur.tableFileId,
		ColumnSets:    [][][]byte{ur.PrevRecord, ur.NewRecord},
	}
	return fields.Serialize()
}
