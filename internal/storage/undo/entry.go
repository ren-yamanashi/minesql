package undo

import "github.com/ren-yamanashi/minesql/internal/storage/lock"

type Entry struct {
	trxId      lock.TrxId
	recordType RecordType
	record     Record
}

func NewEntry(trxId lock.TrxId, recordType RecordType, record Record) Entry {
	return Entry{trxId: trxId, recordType: recordType, record: record}
}

func (e Entry) TrxId() lock.TrxId      { return e.trxId }
func (e Entry) RecordType() RecordType { return e.recordType }
func (e Entry) Record() Record         { return e.record }
