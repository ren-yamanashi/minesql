package undo

import (
	"github.com/ren-yamanashi/minesql/internal/storage/btree"
	"github.com/ren-yamanashi/minesql/internal/storage/lock"
	"github.com/ren-yamanashi/minesql/internal/storage/page"
)

type DeleteRecord struct {
	tableFileId   page.FileId  // テーブルの FileId
	record        btree.Record // 削除したレコード
	prevLastTrxId lock.TrxId
	prevRollPtr   Pointer
}

func NewDeleteRecord(tableFileId page.FileId, record btree.Record, prevLastTrxId lock.TrxId, prevRollPtr Pointer) DeleteRecord {
	return DeleteRecord{
		tableFileId:   tableFileId,
		record:        record,
		prevLastTrxId: prevLastTrxId,
		prevRollPtr:   prevRollPtr,
	}
}

func (dr DeleteRecord) TableFileId() page.FileId { return dr.tableFileId }
func (dr DeleteRecord) Record() btree.Record     { return dr.record }

// serialize は DeleteRecord を バイト列にシリアライズする
func (dr DeleteRecord) serialize(trxId lock.TrxId, undoNum undoNumber) []byte {
	fields := Fields{
		trxId:         trxId,
		undoNum:       undoNum,
		recordType:    RecordTypeDelete,
		prevLastTrxId: dr.prevLastTrxId,
		prevRollPtr:   dr.prevRollPtr,
		tableFileId:   dr.tableFileId,
		columnSets:    [][][]byte{dr.record},
	}
	return fields.serialize()
}
