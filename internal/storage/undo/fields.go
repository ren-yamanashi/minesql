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
	trxId         lock.TrxId
	undoNum       undoNumber
	recordType    recordType
	prevLastTrxId lock.TrxId  // 上書き前のレコードの lastTrxId
	prevRollPtr   Pointer     // 上書き前のレコードの rollPtr
	tableFileId   page.FileId // テーブルの FileId
	columnSets    [][][]byte  // Insert/Delete は 1 Update は 2 セット
}

// TrxId はトランザクション ID を返す
func (f *Fields) TrxId() lock.TrxId { return f.trxId }

// UndoNum は Undo 番号を返す
func (f *Fields) UndoNum() undoNumber { return f.undoNum }

// RecordType はレコードタイプを返す
func (f *Fields) RecordType() recordType { return f.recordType }

// PrevLastTrxId は上書き前のレコードの lastTrxId を返す
func (f *Fields) PrevLastTrxId() lock.TrxId { return f.prevLastTrxId }

// PrevRollPtr は上書き前のレコードの rollPtr を返す
func (f *Fields) PrevRollPtr() Pointer { return f.prevRollPtr }

// TableFileId はテーブルの FileId を返す
func (f *Fields) TableFileId() page.FileId { return f.tableFileId }

// ColumnSets はカラムセットを返す
func (f *Fields) ColumnSets() [][][]byte { return f.columnSets }

// serialize は Undo レコードをバイト列にシリアライズする
//   - return : prevLastTrxId (4B) + prevRollPtr (6B) + tableFileId (4B) + [numColumns (2B) + [colLen (2B) + colData]]...
func (f *Fields) serialize() []byte {
	var data []byte

	// prevLastTrxId, prevRollPtr, tableFileId
	data = binary.BigEndian.AppendUint32(data, uint32(f.prevLastTrxId))
	data = append(data, f.prevRollPtr.Encode()...)
	data = binary.BigEndian.AppendUint32(data, uint32(f.tableFileId))

	// columnSets
	for _, cols := range f.columnSets {
		data = binary.BigEndian.AppendUint16(data, uint16(len(cols)))
		for _, col := range cols {
			data = binary.BigEndian.AppendUint16(data, uint16(len(col)))
			data = append(data, col...)
		}
	}

	// ヘッダー + Data 統合
	buf := make([]byte, recordHeaderSize+len(data))
	binary.BigEndian.PutUint32(buf[headerTrxIdOffset:headerUndoNumOffset], uint32(f.trxId))
	binary.BigEndian.PutUint32(buf[headerUndoNumOffset:headerRecordTypeOffset], f.undoNum)
	buf[headerRecordTypeOffset] = byte(f.recordType)
	binary.BigEndian.PutUint16(buf[headerDataLenOffset:recordHeaderSize], uint16(len(data)))
	copy(buf[recordHeaderSize:], data)
	return buf
}

// DeserializeFields は Undo レコードのバイト列から Fields を復元する
func DeserializeFields(buf []byte) (Fields, error) {
	if len(buf) < recordHeaderSize {
		return Fields{}, errInvalidRecord
	}

	var fields Fields
	fields.trxId = lock.TrxId(binary.BigEndian.Uint32(buf[headerTrxIdOffset:headerUndoNumOffset]))
	fields.undoNum = binary.BigEndian.Uint32(buf[headerUndoNumOffset:headerRecordTypeOffset])
	fields.recordType = recordType(buf[headerRecordTypeOffset])
	dataLen := int(binary.BigEndian.Uint16(buf[headerDataLenOffset:recordHeaderSize]))

	if len(buf) < recordHeaderSize+dataLen {
		return Fields{}, errInvalidRecord
	}

	data := buf[recordHeaderSize : recordHeaderSize+dataLen]
	offset := 0

	// この操作で上書きされる前のレコードが持っていた lastTrxId と rollPtr を復元
	const prevFieldsSize = lock.TrxIdSize + PointerSize
	if offset+prevFieldsSize > len(data) {
		return Fields{}, errInvalidRecord
	}
	fields.prevLastTrxId = lock.TrxId(binary.BigEndian.Uint32(data[offset : offset+lock.TrxIdSize]))
	prevRollPtr, err := DecodePointer(data[offset+lock.TrxIdSize : offset+prevFieldsSize])
	if err != nil {
		return Fields{}, err
	}
	fields.prevRollPtr = prevRollPtr
	offset += prevFieldsSize

	// tableFileId
	if offset+page.FileIdSize > len(data) {
		return Fields{}, errInvalidRecord
	}
	fields.tableFileId = page.FileId(binary.BigEndian.Uint32(data[offset : offset+page.FileIdSize]))
	offset += page.FileIdSize

	// columnSets
	remaining := data[offset:]
	for len(remaining) > 0 {
		columns, n, err := parseColumnSet(remaining)
		if err != nil {
			return Fields{}, err
		}
		fields.columnSets = append(fields.columnSets, columns)
		remaining = remaining[n:]
	}
	return fields, nil
}

// ToRecord は Fields を RecordType に対応する Record に変換する
func (f *Fields) ToRecord() (Record, error) {
	switch f.recordType {
	case RecordTypeInsert:
		if len(f.columnSets) < 1 {
			return nil, errInvalidRecord
		}
		return InsertRecord{
			tableFileId:   f.tableFileId,
			record:        f.columnSets[0],
			prevLastTrxId: f.prevLastTrxId,
			prevRollPtr:   f.prevRollPtr,
		}, nil
	case RecordTypeDelete:
		if len(f.columnSets) < 1 {
			return nil, errInvalidRecord
		}
		return DeleteRecord{
			tableFileId:   f.tableFileId,
			record:        f.columnSets[0],
			prevLastTrxId: f.prevLastTrxId,
			prevRollPtr:   f.prevRollPtr,
		}, nil
	case RecordTypeUpdate:
		if len(f.columnSets) < 2 {
			return nil, errInvalidRecord
		}
		return UpdateRecord{
			tableFileId:   f.tableFileId,
			prevRecord:    f.columnSets[0],
			newRecord:     f.columnSets[1],
			prevLastTrxId: f.prevLastTrxId,
			prevRollPtr:   f.prevRollPtr,
		}, nil
	default:
		return nil, errInvalidRecord
	}
}

// parseColumnSet はバイト列からカラムセット 1 つを読み取り、読み取ったバイト数を返す
func parseColumnSet(data []byte) ([][]byte, int, error) {
	if len(data) < columnCountSize {
		return nil, 0, errInvalidRecord
	}
	numCols := int(binary.BigEndian.Uint16(data[0:columnCountSize]))
	offset := columnCountSize

	columns := make([][]byte, numCols)
	for i := range numCols {
		if offset+columnLenSize > len(data) {
			return nil, 0, errInvalidRecord
		}
		colLen := int(binary.BigEndian.Uint16(data[offset : offset+columnLenSize]))
		offset += columnLenSize
		if offset+colLen > len(data) {
			return nil, 0, errInvalidRecord
		}
		columns[i] = make([]byte, colLen)
		copy(columns[i], data[offset:offset+colLen])
		offset += colLen
	}
	return columns, offset, nil
}
