package undo

import (
	"encoding/binary"
	"errors"
)

// MetaTableType は MetaInsertUndoRecord の対象となるカタログ Meta テーブルの種別
type MetaTableType int

const (
	metaTableTypeUnknown MetaTableType = iota
	MetaTableTypeTable
	MetaTableTypeIndex
	MetaTableTypeIndexKeyColumn
	MetaTableTypeColumn
	MetaTableTypeConstraint
)

func (t MetaTableType) String() string {
	switch t {
	case MetaTableTypeTable:
		return "Table"
	case MetaTableTypeIndex:
		return "Index"
	case MetaTableTypeIndexKeyColumn:
		return "IndexKeyColumn"
	case MetaTableTypeColumn:
		return "Column"
	case MetaTableTypeConstraint:
		return "Constraint"
	default:
		return "Unknown"
	}
}

const (
	metaInsertMetaTableTypeOffset = 0
	metaInsertKeyLenOffset        = 1
	metaInsertHeaderSize          = 3 // MetaTableType(1) + KeyLen(2)
)

// ErrInvalidMetaInsertUndoRecord は MetaInsertUndoRecord のデシリアライズに失敗したことを表す
var ErrInvalidMetaInsertUndoRecord = errors.New("undo: invalid meta insert undo record")

// MetaInsertUndoRecord はカタログ Meta テーブルへのレコード挿入を取り消すための DDL Undo レコード
//   - metaTableType: 対象 Meta テーブルの種別
//   - key: 挿入したレコードの主キー (= Meta テーブルから物理削除する際のキー)
type MetaInsertUndoRecord struct {
	metaTableType MetaTableType
	key           []byte
}

func NewMetaInsertUndoRecord(metaTableType MetaTableType, key []byte) MetaInsertUndoRecord {
	return MetaInsertUndoRecord{metaTableType: metaTableType, key: key}
}

func (r MetaInsertUndoRecord) MetaTableType() MetaTableType { return r.metaTableType }

// Key は内部 key バイト列の独立した copy を返す
//   - 呼び出し側で自由に変更してよい (内部 slice とは独立)
func (r MetaInsertUndoRecord) Key() []byte {
	result := make([]byte, len(r.key))
	copy(result, r.key)
	return result
}

// Serialize は MetaInsertUndoRecord をバイト列にエンコードする
//   - return: MetaTableType(1B) + KeyLen(2B) + Key(N B)
func (r MetaInsertUndoRecord) Serialize() []byte {
	buf := make([]byte, metaInsertHeaderSize+len(r.key))
	buf[metaInsertMetaTableTypeOffset] = byte(r.metaTableType)
	binary.BigEndian.PutUint16(buf[metaInsertKeyLenOffset:metaInsertHeaderSize], uint16(len(r.key)))
	copy(buf[metaInsertHeaderSize:], r.key)
	return buf
}

// DeserializeMetaInsertUndoRecord はバイト列から MetaInsertUndoRecord を復元する
func DeserializeMetaInsertUndoRecord(data []byte) (MetaInsertUndoRecord, error) {
	if len(data) < metaInsertHeaderSize {
		return MetaInsertUndoRecord{}, ErrInvalidMetaInsertUndoRecord
	}
	metaTableType := MetaTableType(data[metaInsertMetaTableTypeOffset])
	keyLen := int(binary.BigEndian.Uint16(data[metaInsertKeyLenOffset:metaInsertHeaderSize]))
	if len(data) < metaInsertHeaderSize+keyLen {
		return MetaInsertUndoRecord{}, ErrInvalidMetaInsertUndoRecord
	}
	key := make([]byte, keyLen)
	copy(key, data[metaInsertHeaderSize:metaInsertHeaderSize+keyLen])
	return MetaInsertUndoRecord{metaTableType: metaTableType, key: key}, nil
}
