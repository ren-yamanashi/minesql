package undo

import (
	"github.com/ren-yamanashi/minesql/internal/storage/btree/node"
	"github.com/ren-yamanashi/minesql/internal/storage/lock"
	"github.com/ren-yamanashi/minesql/internal/storage/page"
)

type InsertRecord struct {
	tableFileId   page.FileId // テーブルの FileId
	Record        node.Record // 挿入したレコード
	PrevLastTrxId lock.TrxId  // INSERT は前バージョンが存在しないため常に 0
	PrevRollPtr   Pointer     // INSERT は前バージョンが存在しないため常に NullPointer
}

func NewInsertRecord(tableFileId page.FileId, record node.Record) InsertRecord {
	return InsertRecord{
		tableFileId:   tableFileId,
		Record:        record,
		PrevLastTrxId: 0,
		PrevRollPtr:   NullPointer,
	}
}

// Serialize は InsertRecord を バイト列にシリアライズする
func (ir InsertRecord) Serialize(trxId lock.TrxId, undoNum UndoNumber) []byte {
	fields := Fields{
		TrxId:         trxId,
		UndoNum:       undoNum,
		RecordType:    RecordTypeInsert,
		PrevLastTrxId: ir.PrevLastTrxId,
		PrevRollPtr:   ir.PrevRollPtr,
		TableFileId:   ir.tableFileId,
		ColumnSets:    [][][]byte{ir.Record},
	}
	return fields.Serialize()
}
