package undo

import (
	"github.com/ren-yamanashi/minesql/internal/storage/btree/node"
	"github.com/ren-yamanashi/minesql/internal/storage/lock"
	"github.com/ren-yamanashi/minesql/internal/storage/page"
)

type DeleteRecord struct {
	tableFileId   page.FileId // テーブルの FileId
	Record        node.Record // 削除したレコード
	PrevLastTrxId lock.TrxId
	PrevRollPtr   Pointer
}

func NewDeleteRecord(tableFileId page.FileId, record node.Record, prevLastTrxId lock.TrxId, prevRollPtr Pointer) DeleteRecord {
	return DeleteRecord{
		tableFileId:   tableFileId,
		Record:        record,
		PrevLastTrxId: prevLastTrxId,
		PrevRollPtr:   prevRollPtr,
	}
}

// Serialize は DeleteRecord を バイト列にシリアライズする
func (dr DeleteRecord) Serialize(trxId lock.TrxId, undoNum UndoNumber) []byte {
	fields := Fields{
		TrxId:         trxId,
		UndoNum:       undoNum,
		RecordType:    RecordTypeDelete,
		PrevLastTrxId: dr.PrevLastTrxId,
		PrevRollPtr:   dr.PrevRollPtr,
		TableFileId:   dr.tableFileId,
		ColumnSets:    [][][]byte{dr.Record},
	}
	return fields.Serialize()
}

func (dr DeleteRecord) TableFileId() page.FileId {
	return dr.tableFileId
}
