package access

// TrxId はトランザクション ID
type TrxId = uint64

// UndoLog は全トランザクションの Undo レコードをトランザクションごとに管理する
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

// PopLast は指定した trxId の Undo ログの最後のレコードを削除する
//
// 書き込み操作が失敗した場合に、事前に記録した Undo レコードを取り消すために使用する
func (u *UndoLog) PopLast(trxId TrxId) {
	records := u.records[trxId]
	if len(records) > 0 {
		u.records[trxId] = records[:len(records)-1]
	}
}

// Discard は指定した trxId の Undo ログを破棄する
func (u *UndoLog) Discard(trxId TrxId) {
	delete(u.records, trxId)
}
