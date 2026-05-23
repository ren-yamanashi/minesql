package redo

import (
	"testing"

	"github.com/ren-yamanashi/minesql/internal/storage/page"
	"github.com/stretchr/testify/assert"
)

func TestRecordSerialize(t *testing.T) {
	t.Run("ページ変更レコードをシリアライズできる", func(t *testing.T) {
		// GIVEN
		pg := buildTestPage(t)
		r := Record{
			lsn:        Lsn(1),
			trxId:      10,
			recordType: RecordTypePageWrite,
			pageId:     page.NewId(page.FileId(2), page.PageNumber(3)),
			data:       *pg,
		}

		// WHEN
		buf := r.serialize()

		// THEN
		assert.Equal(t, recordHeaderSize+page.Size, len(buf))
	})

	t.Run("COMMIT レコードをシリアライズできる", func(t *testing.T) {
		// GIVEN
		r := Record{
			lsn:        Lsn(2),
			trxId:      10,
			recordType: RecordTypeCommit,
		}

		// WHEN
		buf := r.serialize()

		// THEN
		assert.Equal(t, recordHeaderSize, len(buf))
	})

	t.Run("ROLLBACK レコードをシリアライズできる", func(t *testing.T) {
		// GIVEN
		r := Record{
			lsn:        Lsn(3),
			trxId:      10,
			recordType: RecordTypeRollback,
		}

		// WHEN
		buf := r.serialize()

		// THEN
		assert.Equal(t, recordHeaderSize, len(buf))
	})
}

func TestDeserializeRecord(t *testing.T) {
	t.Run("ページ変更レコードのラウンドトリップ", func(t *testing.T) {
		// GIVEN
		pg := buildTestPage(t)
		original := Record{
			lsn:        Lsn(5),
			trxId:      42,
			recordType: RecordTypePageWrite,
			pageId:     page.NewId(page.FileId(1), page.PageNumber(10)),
			data:       *pg,
		}
		buf := original.serialize()

		// WHEN
		decoded, readBytes, err := deserializeRecord(buf)

		// THEN
		assert.NoError(t, err)
		assert.Equal(t, len(buf), readBytes)
		assert.Equal(t, original.Lsn(), decoded.Lsn())
		assert.Equal(t, original.TrxId(), decoded.TrxId())
		assert.Equal(t, original.Type(), decoded.Type())
		assert.Equal(t, original.PageId(), decoded.PageId())
		originalData := original.Data()
		decodedData := decoded.Data()
		assert.Equal(t, originalData.ToBytes(), decodedData.ToBytes())
	})

	t.Run("COMMIT レコードのラウンドトリップ", func(t *testing.T) {
		// GIVEN
		original := Record{
			lsn:        Lsn(6),
			trxId:      42,
			recordType: RecordTypeCommit,
		}
		buf := original.serialize()

		// WHEN
		decoded, readBytes, err := deserializeRecord(buf)

		// THEN
		assert.NoError(t, err)
		assert.Equal(t, len(buf), readBytes)
		assert.Equal(t, original.Lsn(), decoded.Lsn())
		assert.Equal(t, original.TrxId(), decoded.TrxId())
		assert.Equal(t, original.Type(), decoded.Type())
	})

	t.Run("ROLLBACK レコードのラウンドトリップ", func(t *testing.T) {
		// GIVEN
		original := Record{
			lsn:        Lsn(7),
			trxId:      42,
			recordType: RecordTypeRollback,
		}
		buf := original.serialize()

		// WHEN
		decoded, readBytes, err := deserializeRecord(buf)

		// THEN
		assert.NoError(t, err)
		assert.Equal(t, len(buf), readBytes)
		assert.Equal(t, original.Lsn(), decoded.Lsn())
		assert.Equal(t, original.TrxId(), decoded.TrxId())
		assert.Equal(t, original.Type(), decoded.Type())
	})

	t.Run("ヘッダーサイズ未満のデータはエラーを返す", func(t *testing.T) {
		// GIVEN
		data := make([]byte, recordHeaderSize-1)

		// WHEN
		_, _, err := deserializeRecord(data)

		// THEN
		assert.ErrorIs(t, err, errInvalidRecord)
	})

	t.Run("データ長が実データより大きい場合はエラーを返す", func(t *testing.T) {
		// GIVEN
		pg := buildTestPage(t)
		r := Record{
			lsn:        Lsn(1),
			trxId:      1,
			recordType: RecordTypePageWrite,
			pageId:     page.NewId(page.FileId(1), page.PageNumber(1)),
			data:       *pg,
		}
		buf := r.serialize()
		// データ部分を切り詰めてデータ長と実データを不一致にする
		truncated := buf[:recordHeaderSize+10]

		// WHEN
		_, _, err := deserializeRecord(truncated)

		// THEN
		assert.ErrorIs(t, err, errInvalidRecord)
	})

	t.Run("複数レコードが連続するバイト列から 1 件目を読み取れる", func(t *testing.T) {
		// GIVEN
		r1 := Record{lsn: Lsn(1), trxId: 1, recordType: RecordTypeCommit}
		r2 := Record{lsn: Lsn(2), trxId: 2, recordType: RecordTypeCommit}
		buf := append(r1.serialize(), r2.serialize()...)

		// WHEN
		decoded, readBytes, err := deserializeRecord(buf)

		// THEN
		assert.NoError(t, err)
		assert.Equal(t, Lsn(1), decoded.Lsn())
		// readBytes で 2 件目の開始位置が分かる
		decoded2, _, err := deserializeRecord(buf[readBytes:])
		assert.NoError(t, err)
		assert.Equal(t, Lsn(2), decoded2.Lsn())
	})
}

// buildTestPage はテスト用の 4KB ページを作成する
func buildTestPage(t *testing.T) *page.Page {
	t.Helper()
	data := make([]byte, page.Size)
	// テストデータを書き込み
	for i := range data {
		data[i] = byte(i % 256)
	}
	pg, err := page.NewPage(data)
	if err != nil {
		t.Fatalf("テストページの作成に失敗: %v", err)
	}
	return pg
}
