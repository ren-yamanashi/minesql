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
		prevRollPtr:   NullPointer(),
	}
}

func (ir InsertRecord) TableFileId() page.FileId { return ir.tableFileId }
func (ir InsertRecord) Record() btree.Record     { return ir.record }

func (ir InsertRecord) Serialize(trxId lock.TrxId, undoNum UndoNumber) []byte {
	fields := Fields{
		trxId:         trxId,
		undoNum:       undoNum,
		recordType:    RecordTypeInsert,
		prevLastTrxId: ir.prevLastTrxId,
		prevRollPtr:   ir.prevRollPtr,
		tableFileId:   ir.tableFileId,
		columnSets:    [][][]byte{ir.record},
	}
	return fields.Serialize()
}
