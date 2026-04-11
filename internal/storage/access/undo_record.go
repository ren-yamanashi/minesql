package access

import (
	"encoding/binary"
	"errors"
	"minesql/internal/storage/buffer"
	"minesql/internal/storage/lock"
)

// UndoRecord は Undo ログの各レコードが実装するインターフェース
type UndoRecord interface {
	Undo(bp *buffer.BufferPool, trxId lock.TrxId, lockMgr *lock.Manager) error
	Serialize(trxId uint64, undoNo uint64) []byte
}

type UndoRecordType uint8

const (
	UndoInsert        UndoRecordType = 1
	UndoDelete        UndoRecordType = 2
	UndoUpdateInplace UndoRecordType = 3
)

// UNDO レコードのヘッダーサイズ: TrxId(8) + UndoNo(8) + Type(1) + DataLen(2) = 19
const undoRecordHeaderSize = 19

var ErrInvalidUndoRecord = errors.New("invalid undo record")

// SerializeUndoRecord は UNDO レコードをバイト列にシリアライズする
//
// Data のフォーマット:
//   - tableNameLen (2B) + tableName + numColumns (2B) + [colLen (2B) + colData]...
//   - UPDATE_INPLACE の場合は columnSets に prevRecord と newRecord の 2 つを渡す
func SerializeUndoRecord(trxId uint64, undoNo uint64, recordType UndoRecordType, tableName string, columnSets ...[][]byte) []byte {
	// Data 部分をシリアライズ
	var data []byte

	// テーブル名
	tableNameBytes := []byte(tableName)
	data = binary.BigEndian.AppendUint16(data, uint16(len(tableNameBytes)))
	data = append(data, tableNameBytes...)

	// カラムセット
	for _, columns := range columnSets {
		data = binary.BigEndian.AppendUint16(data, uint16(len(columns)))
		for _, col := range columns {
			data = binary.BigEndian.AppendUint16(data, uint16(len(col)))
			data = append(data, col...)
		}
	}

	// ヘッダー + Data を結合
	buf := make([]byte, undoRecordHeaderSize+len(data))
	binary.BigEndian.PutUint64(buf[0:8], trxId)
	binary.BigEndian.PutUint64(buf[8:16], undoNo)
	buf[16] = byte(recordType)
	binary.BigEndian.PutUint16(buf[17:19], uint16(len(data)))
	copy(buf[19:], data)

	return buf
}

// DeserializeUndoRecord は UNDO レコードのバイト列からフィールドを復元する
//
// columnSets は INSERT/DELETE の場合 1 セット、UPDATE_INPLACE の場合 2 セット (prevRecord, newRecord)
func DeserializeUndoRecord(buf []byte) (trxId uint64, undoNo uint64, recordType UndoRecordType, tableName string, columnSets [][][]byte, err error) {
	if len(buf) < undoRecordHeaderSize {
		return 0, 0, 0, "", nil, ErrInvalidUndoRecord
	}

	trxId = binary.BigEndian.Uint64(buf[0:8])
	undoNo = binary.BigEndian.Uint64(buf[8:16])
	recordType = UndoRecordType(buf[16])
	dataLen := int(binary.BigEndian.Uint16(buf[17:19]))

	if len(buf) < undoRecordHeaderSize+dataLen {
		return 0, 0, 0, "", nil, ErrInvalidUndoRecord
	}

	data := buf[19 : 19+dataLen]
	offset := 0

	// テーブル名
	if offset+2 > len(data) {
		return 0, 0, 0, "", nil, ErrInvalidUndoRecord
	}
	tableNameLen := int(binary.BigEndian.Uint16(data[offset : offset+2]))
	offset += 2
	if offset+tableNameLen > len(data) {
		return 0, 0, 0, "", nil, ErrInvalidUndoRecord
	}
	tableName = string(data[offset : offset+tableNameLen])
	offset += tableNameLen

	// カラムセットを読み取る (残りデータがある限り)
	remaining := data[offset:]
	for len(remaining) > 0 {
		columns, n, parseErr := parseColumnSet(remaining)
		if parseErr != nil {
			return 0, 0, 0, "", nil, parseErr
		}
		columnSets = append(columnSets, columns)
		remaining = remaining[n:]
	}

	return trxId, undoNo, recordType, tableName, columnSets, nil
}

// parseColumnSet はバイト列からカラムセット 1 つを読み取り、読み取ったバイト数を返す
func parseColumnSet(data []byte) ([][]byte, int, error) {
	if len(data) < 2 {
		return nil, 0, ErrInvalidUndoRecord
	}
	numCols := int(binary.BigEndian.Uint16(data[0:2]))
	offset := 2

	columns := make([][]byte, numCols)
	for i := range numCols {
		if offset+2 > len(data) {
			return nil, 0, ErrInvalidUndoRecord
		}
		colLen := int(binary.BigEndian.Uint16(data[offset : offset+2]))
		offset += 2
		if offset+colLen > len(data) {
			return nil, 0, ErrInvalidUndoRecord
		}
		columns[i] = make([]byte, colLen)
		copy(columns[i], data[offset:offset+colLen])
		offset += colLen
	}

	return columns, offset, nil
}
