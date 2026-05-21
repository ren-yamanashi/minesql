package undo

import (
	"encoding/binary"

	"github.com/ren-yamanashi/minesql/internal/storage/lock"
	"github.com/ren-yamanashi/minesql/internal/storage/page"
)

const (
	columnCountSize = 2 // カラム数フィールドのバイト数
	columnLenSize   = 2 // カラムデータ長フィールドのバイト数
)

// Fields は Undo ログレコードのシリアライズ/デシリアライズに使用するフィールド群
type Fields struct {
	TrxId         lock.TrxId
	UndoNum       UndoNumber
	RecordType    RecordType
	PrevLastTrxId lock.TrxId  // 上書き前のレコードの lastTrxId
	PrevRollPtr   Pointer     // 上書き前のレコードの rollPtr
	TableFileId   page.FileId // テーブルの FileId
	ColumnSets    [][][]byte  // Insert/Delete は 1 Update は 2 セット
}

// Serialize は Undo レコードをバイト列にシリアライズする
//   - return : prevLastTrxId (4B) + prevRollPtr (6B) + tableFileId (4B) + [numColumns (2B) + [colLen (2B) + colData]]...
func (f *Fields) Serialize() []byte {
	var data []byte

	// prevLastTrxId, prevRollPtr, tableFileId
	data = binary.BigEndian.AppendUint32(data, f.PrevLastTrxId)
	data = append(data, f.PrevRollPtr.Encode()...)
	data = binary.BigEndian.AppendUint32(data, uint32(f.TableFileId))

	// columnSets
	for _, cols := range f.ColumnSets {
		data = binary.BigEndian.AppendUint16(data, uint16(len(cols)))
		for _, col := range cols {
			data = binary.BigEndian.AppendUint16(data, uint16(len(col)))
			data = append(data, col...)
		}
	}

	// ヘッダー + Data 統合
	buf := make([]byte, recordHeaderSize+len(data))
	binary.BigEndian.PutUint32(buf[headerTrxIdOffset:headerUndoNumOffset], f.TrxId)
	binary.BigEndian.PutUint32(buf[headerUndoNumOffset:headerRecordTypeOffset], f.UndoNum)
	buf[headerRecordTypeOffset] = byte(f.RecordType)
	binary.BigEndian.PutUint16(buf[headerDataLenOffset:recordHeaderSize], uint16(len(data)))
	copy(buf[recordHeaderSize:], data)
	return buf
}

// DeserializeFields は Undo レコードのバイト列から Fields を復元する
func DeserializeFields(buf []byte) (Fields, error) {
	if len(buf) < recordHeaderSize {
		return Fields{}, ErrInvalidRecord
	}

	var fields Fields
	fields.TrxId = binary.BigEndian.Uint32(buf[headerTrxIdOffset:headerUndoNumOffset])
	fields.UndoNum = binary.BigEndian.Uint32(buf[headerUndoNumOffset:headerRecordTypeOffset])
	fields.RecordType = RecordType(buf[headerRecordTypeOffset])
	dataLen := int(binary.BigEndian.Uint16(buf[headerDataLenOffset:recordHeaderSize]))

	if len(buf) < recordHeaderSize+dataLen {
		return Fields{}, ErrInvalidRecord
	}

	data := buf[recordHeaderSize : recordHeaderSize+dataLen]
	offset := 0

	// この操作で上書きされる前のレコードが持っていた lastTrxId と rollPtr を復元
	const prevFieldsSize = lock.TrxIdSize + PointerSize
	if offset+prevFieldsSize > len(data) {
		return Fields{}, ErrInvalidRecord
	}
	fields.PrevLastTrxId = binary.BigEndian.Uint32(data[offset : offset+lock.TrxIdSize])
	prevRollPtr, err := DecodePointer(data[offset+lock.TrxIdSize : offset+prevFieldsSize])
	if err != nil {
		return Fields{}, err
	}
	fields.PrevRollPtr = prevRollPtr
	offset += prevFieldsSize

	// TableFileId
	if offset+page.FileIdSize > len(data) {
		return Fields{}, ErrInvalidRecord
	}
	fields.TableFileId = page.FileId(binary.BigEndian.Uint32(data[offset : offset+page.FileIdSize]))
	offset += page.FileIdSize

	// ColumnSets
	remaining := data[offset:]
	for len(remaining) > 0 {
		columns, n, err := parseColumnSet(remaining)
		if err != nil {
			return Fields{}, err
		}
		fields.ColumnSets = append(fields.ColumnSets, columns)
		remaining = remaining[n:]
	}
	return fields, nil
}

// parseColumnSet はバイト列からカラムセット 1 つを読み取り、読み取ったバイト数を返す
func parseColumnSet(data []byte) ([][]byte, int, error) {
	if len(data) < columnCountSize {
		return nil, 0, ErrInvalidRecord
	}
	numCols := int(binary.BigEndian.Uint16(data[0:columnCountSize]))
	offset := columnCountSize

	columns := make([][]byte, numCols)
	for i := range numCols {
		if offset+columnLenSize > len(data) {
			return nil, 0, ErrInvalidRecord
		}
		colLen := int(binary.BigEndian.Uint16(data[offset : offset+columnLenSize]))
		offset += columnLenSize
		if offset+colLen > len(data) {
			return nil, 0, ErrInvalidRecord
		}
		columns[i] = make([]byte, colLen)
		copy(columns[i], data[offset:offset+colLen])
		offset += colLen
	}
	return columns, offset, nil
}
