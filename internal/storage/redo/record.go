package redo

import (
	"encoding/binary"
	"errors"

	"github.com/ren-yamanashi/minesql/internal/storage/lock"
	"github.com/ren-yamanashi/minesql/internal/storage/page"
)

type (
	Lsn        uint32 // Lsn はログシーケンス番号 (Redo ログレコードを一意に識別するための番号)
	RecordType int
)

const (
	recordTypeUnknown RecordType = iota
	RecordTypePageWrite
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
	lsn        Lsn
	trxId      lock.TrxId // 変更を行ったトランザクション ID
	recordType RecordType
	pageId     page.Id    // 変更対象のページ (COMMIT/ROLLBACK の場合はゼロ値)
	data       *page.Page // 変更対象ページのコピー (COMMIT/ROLLBACK の場合は nil)
}

func (r Record) Lsn() Lsn          { return r.lsn }
func (r Record) TrxId() lock.TrxId { return r.trxId }
func (r Record) Type() RecordType  { return r.recordType }
func (r Record) PageId() page.Id   { return r.pageId }
func (r Record) Data() *page.Page  { return r.data.Copy() } // Record はログレコードとして不変であるべきため、内部ポインタを直接返さない

func (r Record) Serialize() []byte {
	var pageBytes []byte
	if !r.data.IsZero() {
		pageBytes = r.data.Bytes()
	}
	dataLen := len(pageBytes)
	buf := make([]byte, recordHeaderSize+dataLen)

	binary.BigEndian.PutUint32(buf[recordHeaderLsnOffset:recordHeaderTrxOffset], uint32(r.lsn))
	binary.BigEndian.PutUint32(buf[recordHeaderTrxOffset:recordHeaderRecordTypeOffset], uint32(r.trxId))
	buf[recordHeaderRecordTypeOffset] = byte(r.recordType)
	r.pageId.WriteAt(buf, recordHeaderPageIdOffset)
	binary.BigEndian.PutUint16(buf[recordHeaderDataLenOffset:recordHeaderSize], uint16(dataLen))

	copy(buf[recordHeaderSize:], pageBytes)

	return buf
}

func (r Record) Size() int {
	if !r.data.IsZero() {
		return recordHeaderSize + page.Size
	}
	return recordHeaderSize
}

// DeserializeRecord はバイト列から Record をデシリアライズする
//   - return: デシリアライズした Record, 読み取ったバイト数, エラー
func DeserializeRecord(data []byte) (Record, int, error) {
	if len(data) < recordHeaderSize {
		return Record{}, 0, ErrInvalidRecord
	}

	lsn := Lsn(binary.BigEndian.Uint32(data[recordHeaderLsnOffset:recordHeaderTrxOffset]))
	trxId := lock.TrxId(binary.BigEndian.Uint32(data[recordHeaderTrxOffset:recordHeaderRecordTypeOffset]))
	recordType := RecordType(data[recordHeaderRecordTypeOffset])
	switch recordType {
	case RecordTypePageWrite, RecordTypeCommit, RecordTypeRollback:
	default:
		return Record{}, 0, ErrInvalidRecord
	}
	pageId := page.ReadId(data, recordHeaderPageIdOffset)
	dataLen := int(binary.BigEndian.Uint16(data[recordHeaderDataLenOffset:recordHeaderSize]))
	totalLen := recordHeaderSize + dataLen

	if len(data) < totalLen {
		return Record{}, 0, ErrInvalidRecord
	}

	// ページデータがある場合のみデコード (COMMIT/ROLLBACK はページデータなし)
	var pg *page.Page
	if dataLen > 0 {
		recordData := make([]byte, dataLen)
		copy(recordData, data[recordHeaderSize:recordHeaderSize+dataLen])
		p, err := page.NewPage(recordData)
		if err != nil {
			return Record{}, 0, err
		}
		pg = p
	}

	return Record{
		lsn:        lsn,
		trxId:      trxId,
		recordType: recordType,
		pageId:     pageId,
		data:       pg,
	}, totalLen, nil
}
