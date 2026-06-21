package undo

import (
	"encoding/binary"
	"errors"
)

// DDLRecordType は DDL Undo レコードの種別
type DDLRecordType int

const (
	ddlRecordTypeUnknown DDLRecordType = iota
	// DDLRecordTypeCreateBTree は B+Tree 作成に対する DDL Undo の種別
	DDLRecordTypeCreateBTree
	// DDLRecordTypeMetaInsert はメタテーブルへのレコード挿入に対する DDL Undo の種別
	DDLRecordTypeMetaInsert
	// DDLRecordTypeAllocateFileId は物理ファイル確保に対する DDL Undo の種別
	DDLRecordTypeAllocateFileId
)

func (t DDLRecordType) String() string {
	switch t {
	case DDLRecordTypeCreateBTree:
		return "CreateBTree"
	case DDLRecordTypeMetaInsert:
		return "MetaInsert"
	case DDLRecordTypeAllocateFileId:
		return "AllocateFileId"
	default:
		return "Unknown"
	}
}

const (
	ddlRecordTypeOffset       = 0
	ddlRecordPayloadLenOffset = 1
	ddlRecordHeaderSize       = 3 // RecordType(1) + PayloadLen(2)
)

// ErrInvalidDDLRecord は DDL Undo レコードのデシリアライズに失敗したことを表す
var ErrInvalidDDLRecord = errors.New("undo: invalid ddl record")

// DDLRecord は DDL Undo 専用領域に書き込まれる 1 件分のレコード
//   - recordType: レコード種別 (B+Tree 解放 / メタテーブル削除 / 物理ファイル削除)
//   - payload: 種別ごとの取消情報のバイト列。 種別固有のエンコードは呼び出し側が担う
type DDLRecord struct {
	recordType DDLRecordType
	payload    []byte
}

func NewDDLRecord(recordType DDLRecordType, payload []byte) DDLRecord {
	return DDLRecord{recordType: recordType, payload: payload}
}

func (r DDLRecord) RecordType() DDLRecordType { return r.recordType }

// Payload は内部 payload バイト列の独立した copy を返す
//   - 呼び出し側で自由に変更してよい (内部 slice とは独立)
func (r DDLRecord) Payload() []byte {
	result := make([]byte, len(r.payload))
	copy(result, r.payload)
	return result
}

// Serialize は DDL Undo レコードをバイト列にエンコードする
//   - return: RecordType(1B) + PayloadLen(2B) + Payload(N B)
func (r DDLRecord) Serialize() []byte {
	buf := make([]byte, ddlRecordHeaderSize+len(r.payload))
	buf[ddlRecordTypeOffset] = byte(r.recordType)
	binary.BigEndian.PutUint16(buf[ddlRecordPayloadLenOffset:ddlRecordHeaderSize], uint16(len(r.payload)))
	copy(buf[ddlRecordHeaderSize:], r.payload)
	return buf
}

// DeserializeDDLRecord はバイト列の先頭から DDL Undo レコードを 1 件読み取り、 消費バイト数を返す
func DeserializeDDLRecord(data []byte) (DDLRecord, int, error) {
	if len(data) < ddlRecordHeaderSize {
		return DDLRecord{}, 0, ErrInvalidDDLRecord
	}
	recordType := DDLRecordType(data[ddlRecordTypeOffset])
	payloadLen := int(binary.BigEndian.Uint16(data[ddlRecordPayloadLenOffset:ddlRecordHeaderSize]))
	totalLen := ddlRecordHeaderSize + payloadLen
	if len(data) < totalLen {
		return DDLRecord{}, 0, ErrInvalidDDLRecord
	}
	payload := make([]byte, payloadLen)
	copy(payload, data[ddlRecordHeaderSize:totalLen])
	return DDLRecord{recordType: recordType, payload: payload}, totalLen, nil
}
