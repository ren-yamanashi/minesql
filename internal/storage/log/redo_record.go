package log

import (
	"encoding/binary"
	"errors"

	"github.com/ren-yamanashi/minesql/internal/storage/page"
)

type RedoRecordType uint8

const (
	RedoPageWrite RedoRecordType = 0 // ページ変更 (ページ全体のコピー)
	RedoCommit    RedoRecordType = 1 // COMMIT マーカー
	RedoRollback  RedoRecordType = 2 // ROLLBACK マーカー
)

// RedoRecord は REDO ログの 1 レコードを表す
type RedoRecord struct {
	LSN    LSN            // このレコードの LSN
	TrxId  uint64         // 変更を行ったトランザクションの ID
	Type   RedoRecordType // レコード種別 (ページ変更, COMMIT, ROLLBACK)
	PageId page.PageId    // 変更対象のページ (COMMIT/ROLLBACK の場合はゼロ値)
	Data   []byte         // ページ全体のコピー (4096B)。COMMIT/ROLLBACK の場合は nil
}

// シリアライズ形式:
//   - LSN:      4 バイト (uint32)
//   - TrxId:    8 バイト (uint64)
//   - Type:     1 バイト (uint8)
//   - PageId:   8 バイト (FileId 4B + PageNumber 4B)
//   - DataLen:  2 バイト (uint16)
//   - Data:     可変長

const redoRecordHeaderSize = 4 + 8 + 1 + 8 + 2 // 23 バイト

var ErrInvalidRedoRecord = errors.New("invalid redo record")

// Serialize は RedoRecord をバイト列にシリアライズする
func (r *RedoRecord) Serialize() []byte {
	dataLen := len(r.Data)
	buf := make([]byte, redoRecordHeaderSize+dataLen)

	binary.BigEndian.PutUint32(buf[0:4], uint32(r.LSN))
	binary.BigEndian.PutUint64(buf[4:12], r.TrxId)
	buf[12] = byte(r.Type)
	r.PageId.WriteTo(buf, 13)
	binary.BigEndian.PutUint16(buf[21:23], uint16(dataLen))

	copy(buf[23:], r.Data)

	return buf
}

// DeserializeRedoRecord はバイト列から RedoRecord をデシリアライズする
//
// 戻り値: デシリアライズした RedoRecord, 読み取ったバイト数, エラー
func DeserializeRedoRecord(data []byte) (RedoRecord, int, error) {
	if len(data) < redoRecordHeaderSize {
		return RedoRecord{}, 0, ErrInvalidRedoRecord
	}

	lsn := LSN(binary.BigEndian.Uint32(data[0:4]))
	trxId := binary.BigEndian.Uint64(data[4:12])
	recordType := RedoRecordType(data[12])
	pageId := page.ReadPageIdFromPageData(data, 13)
	dataLen := int(binary.BigEndian.Uint16(data[21:23]))

	totalLen := redoRecordHeaderSize + dataLen

	// データ長が実際のバイト列の長さを超えていないかチェック
	if len(data) < totalLen {
		return RedoRecord{}, 0, ErrInvalidRedoRecord
	}

	// ページ変更レコードの場合はページデータをコピーする。COMMIT/ROLLBACK の場合は nil のまま
	var recordData []byte
	if dataLen > 0 {
		recordData = make([]byte, dataLen)
		copy(recordData, data[23:23+dataLen])
	}

	return RedoRecord{
		LSN:    lsn,
		TrxId:  trxId,
		Type:   recordType,
		PageId: pageId,
		Data:   recordData,
	}, totalLen, nil
}
