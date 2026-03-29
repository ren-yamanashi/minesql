package undo

// TrxId はトランザクション ID
type TrxId uint64

// UndoLog は全トランザクションの Undo レコードを trxId ごとに管理する
type UndoLog struct {
	records map[TrxId][]LogRecord
}

func NewUndoLog() *UndoLog {
	return &UndoLog{
		records: make(map[TrxId][]LogRecord),
	}
}

// Append は指定した trxId の Undo ログにレコードを追加する
func (u *UndoLog) Append(trxId TrxId, record LogRecord) {
	u.records[trxId] = append(u.records[trxId], record)
}

// GetRecords は指定した trxId の Undo ログレコードを取得する
func (u *UndoLog) GetRecords(trxId TrxId) []LogRecord {
	return u.records[trxId]
}

// Discard は指定した trxId の Undo ログを破棄する
func (u *UndoLog) Discard(trxId TrxId) {
	delete(u.records, trxId)
}
