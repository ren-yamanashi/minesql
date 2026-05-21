package redo

import (
	"encoding/binary"
	"errors"

	"github.com/ren-yamanashi/minesql/internal/storage/lock"
	"github.com/ren-yamanashi/minesql/internal/storage/page"
)

type RecordType int

const (
	RecordTypePageWrite RecordType = iota + 1
	RecordTypeCommit
	RecordTypeRollback
)

const (
	recordHeaderLsnOffset        = 0
	recordHeaderTrxOffset        = 4
	recordHeaderRecordTypeOffset = 8
	recordHeaderPageIdOffset     = 9
	recordHeaderDataLenOffset    = 17
	recordHeaderSize             = 19
)

var ErrInvalidRecord = errors.New("redo: invalid record")

type Record struct {
	Lsn    Lsn
	TrxId  lock.TrxId // 変更を行ったトランザクション ID
	Type   RecordType
	PageId page.PageId // 変更対象のページ (COMMIT/ROLLBACK の場合はゼロ値)
	Data   page.Page   // 変更対象ページ全体のコピー (COMMIT/ROLLBACK の場合はゼロ値)
}

// Serialize は Record をバイト列にシリアライズする
func (r *Record) Serialize() []byte {
	var pageBytes []byte
	if r.Data.Header != nil {
		pageBytes = r.Data.ToBytes()
	}
	dataLen := len(pageBytes)
	buf := make([]byte, recordHeaderSize+dataLen)

	binary.BigEndian.PutUint32(buf[recordHeaderLsnOffset:recordHeaderTrxOffset], uint32(r.Lsn))
	binary.BigEndian.PutUint32(buf[recordHeaderTrxOffset:recordHeaderRecordTypeOffset], r.TrxId)
	buf[recordHeaderRecordTypeOffset] = byte(r.Type)
	r.PageId.WriteTo(buf, recordHeaderPageIdOffset)
	binary.BigEndian.PutUint16(buf[recordHeaderDataLenOffset:recordHeaderSize], uint16(dataLen))

	copy(buf[recordHeaderSize:], pageBytes)

	return buf
}

// DeserializeRecord はバイト列から Record をデシリアライズする
//   - return: デシリアライズした Record, 読み取ったバイト数, エラー
func DeserializeRecord(data []byte) (Record, int, error) {
	if len(data) < recordHeaderSize {
		return Record{}, 0, ErrInvalidRecord
	}

	lsn := Lsn(binary.BigEndian.Uint32(data[recordHeaderLsnOffset:recordHeaderTrxOffset]))
	trxId := binary.BigEndian.Uint32(data[recordHeaderTrxOffset:recordHeaderRecordTypeOffset])
	recordType := RecordType(data[recordHeaderRecordTypeOffset])
	pageId := page.ReadPageId(data, recordHeaderPageIdOffset)
	dataLen := int(binary.BigEndian.Uint16(data[recordHeaderDataLenOffset:recordHeaderSize]))
	totalLen := recordHeaderSize + dataLen

	if len(data) < totalLen {
		return Record{}, 0, ErrInvalidRecord
	}

	// ページデータがある場合のみデコード (COMMIT/ROLLBACK はページデータなし)
	var pg page.Page
	if dataLen > 0 {
		recordData := make([]byte, dataLen)
		copy(recordData, data[recordHeaderSize:recordHeaderSize+dataLen])
		p, err := page.NewPage(recordData)
		if err != nil {
			return Record{}, 0, err
		}
		pg = *p
	}

	return Record{
		Lsn:    lsn,
		TrxId:  trxId,
		Type:   recordType,
		PageId: pageId,
		Data:   pg,
	}, totalLen, nil
}
