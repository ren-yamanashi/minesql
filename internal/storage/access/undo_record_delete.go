package access

import (
	"minesql/internal/storage/buffer"
	"minesql/internal/storage/lock"
)

type UndoDeleteRecord struct {
	table            *Table
	Record           [][]byte
	PrevLastModified TrxId
	PrevRollPtr      UndoPtr
}

func NewUndoDeleteRecord(table *Table, record [][]byte, prevLastModified TrxId, prevRollPtr UndoPtr) UndoDeleteRecord {
	return UndoDeleteRecord{
		table:            table,
		Record:           record,
		PrevLastModified: prevLastModified,
		PrevRollPtr:      prevRollPtr,
	}
}

// Undo は Delete したレコードを挿入する
func (r UndoDeleteRecord) Undo(bp *buffer.BufferPool, trxId lock.TrxId, lockMgr *lock.Manager) error {
	return r.table.insert(bp, trxId, lockMgr, r.Record, NullUndoPtr)
}

// Serialize は UndoDeleteRecord をバイト列にシリアライズする
func (r UndoDeleteRecord) Serialize(trxId uint64, undoNo uint64) []byte {
	return SerializeUndoRecord(UndoRecordFields{
		TrxId:            trxId,
		UndoNo:           undoNo,
		RecordType:       UndoDelete,
		PrevLastModified: r.PrevLastModified,
		PrevRollPtr:      r.PrevRollPtr,
		TableName:        r.table.Name,
		ColumnSets:       [][][]byte{r.Record},
	})
}
