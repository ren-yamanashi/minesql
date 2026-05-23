package redo

import (
	"testing"

	"github.com/ren-yamanashi/minesql/internal/storage/lock"
	"github.com/ren-yamanashi/minesql/internal/storage/page"
	"github.com/stretchr/testify/assert"
)

func TestRecordLsn(t *testing.T) {
	t.Run("設定した LSN を返す", func(t *testing.T) {
		// GIVEN
		r := Record{lsn: Lsn(42)}

		// WHEN
		got := r.Lsn()

		// THEN
		assert.Equal(t, Lsn(42), got)
	})

	t.Run("ゼロ値の LSN を返す", func(t *testing.T) {
		// GIVEN
		r := Record{}

		// WHEN
		got := r.Lsn()

		// THEN
		assert.Equal(t, Lsn(0), got)
	})
}

func TestRecordTrxId(t *testing.T) {
	t.Run("設定したトランザクション ID を返す", func(t *testing.T) {
		// GIVEN
		r := Record{trxId: 100}

		// WHEN
		got := r.TrxId()

		// THEN
		assert.Equal(t, lock.TrxId(100), got)
	})
}

func TestRecordType(t *testing.T) {
	t.Run("PageWrite タイプを返す", func(t *testing.T) {
		// GIVEN
		r := Record{recordType: RecordTypePageWrite}

		// WHEN
		got := r.Type()

		// THEN
		assert.Equal(t, RecordTypePageWrite, got)
	})

	t.Run("Commit タイプを返す", func(t *testing.T) {
		// GIVEN
		r := Record{recordType: RecordTypeCommit}

		// WHEN
		got := r.Type()

		// THEN
		assert.Equal(t, RecordTypeCommit, got)
	})

	t.Run("Rollback タイプを返す", func(t *testing.T) {
		// GIVEN
		r := Record{recordType: RecordTypeRollback}

		// WHEN
		got := r.Type()

		// THEN
		assert.Equal(t, RecordTypeRollback, got)
	})
}

func TestRecordPageId(t *testing.T) {
	t.Run("設定したページ ID を返す", func(t *testing.T) {
		// GIVEN
		pid := page.NewId(page.FileId(5), page.PageNumber(10))
		r := Record{pageId: pid}

		// WHEN
		got := r.PageId()

		// THEN
		assert.Equal(t, pid, got)
	})

	t.Run("ゼロ値のページ ID を返す", func(t *testing.T) {
		// GIVEN
		r := Record{}

		// WHEN
		got := r.PageId()

		// THEN
		assert.Equal(t, page.Id{}, got)
	})
}

func TestRecordData(t *testing.T) {
	t.Run("設定したページデータを返す", func(t *testing.T) {
		// GIVEN
		pg := buildTestPage(t)
		r := Record{data: *pg}

		// WHEN
		got := r.Data()

		// THEN
		assert.Equal(t, pg.ToBytes(), got.ToBytes())
	})

	t.Run("データなしの場合はゼロ値のページを返す", func(t *testing.T) {
		// GIVEN
		r := Record{}

		// WHEN
		got := r.Data()

		// THEN
		assert.Nil(t, got.Header)
	})
}

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
		buf := r.Serialize()

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
		buf := r.Serialize()

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
		buf := r.Serialize()

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
		buf := original.Serialize()

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
		buf := original.Serialize()

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
		buf := original.Serialize()

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
		assert.ErrorIs(t, err, ErrInvalidRecord)
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
		buf := r.Serialize()
		// データ部分を切り詰めてデータ長と実データを不一致にする
		truncated := buf[:recordHeaderSize+10]

		// WHEN
		_, _, err := deserializeRecord(truncated)

		// THEN
		assert.ErrorIs(t, err, ErrInvalidRecord)
	})

	t.Run("複数レコードが連続するバイト列から 1 件目を読み取れる", func(t *testing.T) {
		// GIVEN
		r1 := Record{lsn: Lsn(1), trxId: 1, recordType: RecordTypeCommit}
		r2 := Record{lsn: Lsn(2), trxId: 2, recordType: RecordTypeCommit}
		buf := append(r1.Serialize(), r2.Serialize()...)

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

	t.Run("空のバイト列はエラーを返す", func(t *testing.T) {
		// GIVEN
		data := []byte{}

		// WHEN
		_, _, err := deserializeRecord(data)

		// THEN
		assert.ErrorIs(t, err, ErrInvalidRecord)
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
