package undo

import (
	"github.com/ren-yamanashi/minesql/internal/storage/btree"
	"github.com/ren-yamanashi/minesql/internal/storage/lock"
	"github.com/ren-yamanashi/minesql/internal/storage/page"
)

type InsertRecord struct {
	tableFileId   page.FileId  // テーブルの FileId
	record        btree.Record // 挿入したレコード
	prevLastTrxId lock.TrxId   // INSERT は前バージョンが存在しないため常に 0
	prevRollPtr   Pointer      // INSERT は前バージョンが存在しないため常に NullPointer
}

func NewInsertRecord(tableFileId page.FileId, record btree.Record) InsertRecord {
	return InsertRecord{
		tableFileId:   tableFileId,
		record:        record,
		prevLastTrxId: 0,
		prevRollPtr:   NullPointer,
	}
}

// TableFileId はテーブルの FileId を返す
func (ir InsertRecord) TableFileId() page.FileId {
	return ir.tableFileId
}

// Record は挿入したレコードを返す
func (ir InsertRecord) Record() btree.Record {
	return ir.record
}

// serialize は InsertRecord を バイト列にシリアライズする
func (ir InsertRecord) serialize(trxId lock.TrxId, undoNum undoNumber) []byte {
	fields := Fields{
		TrxId:         trxId,
		UndoNum:       undoNum,
		RecordType:    RecordTypeInsert,
		PrevLastTrxId: ir.prevLastTrxId,
		PrevRollPtr:   ir.prevRollPtr,
		TableFileId:   ir.tableFileId,
		ColumnSets:    [][][]byte{ir.record},
	}
	return fields.Serialize()
}
