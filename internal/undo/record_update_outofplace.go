package undo

import "minesql/internal/access"

type UpdateOutofplaceLogRecord struct {
	InsertLogRecord InsertLogRecord
	DeleteLogRecord DeleteLogRecord
}

func NewUpdateOutofplaceLogRecord(table *access.TableAccessMethod, prevRecord, newRecord [][]byte) UpdateOutofplaceLogRecord {
	return UpdateOutofplaceLogRecord{
		InsertLogRecord: NewInsertLogRecord(table, newRecord),
		DeleteLogRecord: NewDeleteLogRecord(table, prevRecord),
	}
}

// Undo は UpdateOutofplace したレコードを元の値に戻す
// (Outofplace の場合は SoftDelete -> Insert の順で実行されるので Insert -> Delete の順で Undo を実行する)
func (r UpdateOutofplaceLogRecord) Undo() error {
	if err := r.InsertLogRecord.Undo(); err != nil {
		return err
	}
	return r.DeleteLogRecord.Undo()
}
