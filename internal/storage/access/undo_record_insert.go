package access

import (
	"minesql/internal/storage/buffer"
	"minesql/internal/storage/lock"
)

type UndoInsertRecord struct {
	table            *Table
	Record           [][]byte
	PrevLastModified TrxId   // INSERT は前バージョンが存在しないため常に 0
	PrevRollPtr      UndoPtr // INSERT は前バージョンが存在しないため常に NullUndoPtr
}

func NewUndoInsertRecord(table *Table, record [][]byte) UndoInsertRecord {
	return UndoInsertRecord{
		table:            table,
		Record:           record,
		PrevLastModified: 0,
		PrevRollPtr:      NullUndoPtr,
	}
}

// Undo は Insert したレコードを物理削除する
func (r UndoInsertRecord) Undo(bp *buffer.BufferPool, trxId lock.TrxId, lockMgr *lock.Manager) error {
	return r.table.delete(bp, trxId, lockMgr, r.Record) // delete は UndoPtr 不要 (物理削除)
}

// Serialize は UndoInsertRecord をバイト列にシリアライズする
func (r UndoInsertRecord) Serialize(trxId uint64, undoNo uint64) []byte {
	return SerializeUndoRecord(UndoRecordFields{
		TrxId:            trxId,
		UndoNo:           undoNo,
		RecordType:       UndoInsert,
		PrevLastModified: r.PrevLastModified,
		PrevRollPtr:      r.PrevRollPtr,
		TableName:        r.table.Name,
		ColumnSets:       [][][]byte{r.Record},
	})
}
