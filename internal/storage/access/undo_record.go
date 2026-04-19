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

// UndoRecordFields は UNDO レコードのシリアライズ/デシリアライズに使用するフィールド群
type UndoRecordFields struct {
	TrxId            uint64
	UndoNo           uint64
	RecordType       UndoRecordType
	PrevLastModified lock.TrxId // 上書き前の行の lastModified
	PrevRollPtr      UndoPtr    // 上書き前の行の rollPtr
	TableName        string
	ColumnSets       [][][]byte // INSERT/DELETE は 1 セット、UPDATE_INPLACE は 2 セット (prevRecord, newRecord)
}

// SerializeUndoRecord は UNDO レコードをバイト列にシリアライズする
//
// Data のフォーマット:
//   - prevLastModified (8B) + prevRollPtr (4B) + tableNameLen (2B) + tableName + numColumns (2B) + [colLen (2B) + colData]...
func SerializeUndoRecord(uFields UndoRecordFields) []byte {
	// Data 部分をシリアライズ
	var data []byte

	// この操作で上書きされる前の行が持っていた lastModified と rollPtr を記録する
	// undo チェーンを辿って旧バージョンを復元する際に、さらに前のバージョンへの参照として使用する
	data = binary.BigEndian.AppendUint64(data, uFields.PrevLastModified)
	data = append(data, uFields.PrevRollPtr.Encode()...)

	// テーブル名
	tableNameBytes := []byte(uFields.TableName)
	data = binary.BigEndian.AppendUint16(data, uint16(len(tableNameBytes)))
	data = append(data, tableNameBytes...)

	// カラムセット
	for _, columns := range uFields.ColumnSets {
		data = binary.BigEndian.AppendUint16(data, uint16(len(columns)))
		for _, col := range columns {
			data = binary.BigEndian.AppendUint16(data, uint16(len(col)))
			data = append(data, col...)
		}
	}

	// ヘッダー + Data を結合
	buf := make([]byte, undoRecordHeaderSize+len(data))
	binary.BigEndian.PutUint64(buf[0:8], uFields.TrxId)
	binary.BigEndian.PutUint64(buf[8:16], uFields.UndoNo)
	buf[16] = byte(uFields.RecordType)
	binary.BigEndian.PutUint16(buf[17:19], uint16(len(data)))
	copy(buf[19:], data)

	return buf
}

// DeserializeUndoRecord は UNDO レコードのバイト列から UndoRecordFields を復元する
func DeserializeUndoRecord(buf []byte) (UndoRecordFields, error) {
	if len(buf) < undoRecordHeaderSize {
		return UndoRecordFields{}, ErrInvalidUndoRecord
	}

	var uFields UndoRecordFields
	uFields.TrxId = binary.BigEndian.Uint64(buf[0:8])
	uFields.UndoNo = binary.BigEndian.Uint64(buf[8:16])
	uFields.RecordType = UndoRecordType(buf[16])
	dataLen := int(binary.BigEndian.Uint16(buf[17:19]))

	if len(buf) < undoRecordHeaderSize+dataLen {
		return UndoRecordFields{}, ErrInvalidUndoRecord
	}

	data := buf[19 : 19+dataLen]
	offset := 0

	// この操作で上書きされる前の行が持っていた lastModified と rollPtr を復元する
	const prevFieldsSize = 8 + UndoPtrSize
	if offset+prevFieldsSize > len(data) {
		return UndoRecordFields{}, ErrInvalidUndoRecord
	}
	uFields.PrevLastModified = binary.BigEndian.Uint64(data[offset : offset+8])
	uFields.PrevRollPtr, _ = DecodeUndoPtr(data[offset+8 : offset+prevFieldsSize])
	offset += prevFieldsSize

	// テーブル名
	if offset+2 > len(data) {
		return UndoRecordFields{}, ErrInvalidUndoRecord
	}
	tableNameLen := int(binary.BigEndian.Uint16(data[offset : offset+2]))
	offset += 2
	if offset+tableNameLen > len(data) {
		return UndoRecordFields{}, ErrInvalidUndoRecord
	}
	uFields.TableName = string(data[offset : offset+tableNameLen])
	offset += tableNameLen

	// カラムセットを読み取る (残りデータがある限り)
	remaining := data[offset:]
	for len(remaining) > 0 {
		columns, n, parseErr := parseColumnSet(remaining)
		if parseErr != nil {
			return UndoRecordFields{}, parseErr
		}
		uFields.ColumnSets = append(uFields.ColumnSets, columns)
		remaining = remaining[n:]
	}

	return uFields, nil
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
