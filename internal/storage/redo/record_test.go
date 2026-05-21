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
			Lsn:    Lsn(1),
			TrxId:  10,
			Type:   RecordTypePageWrite,
			PageId: page.NewPageId(page.FileId(2), page.PageNumber(3)),
			Data:   *pg,
		}

		// WHEN
		buf := r.Serialize()

		// THEN
		assert.Equal(t, recordHeaderSize+page.PageSize, len(buf))
	})

	t.Run("COMMIT レコードをシリアライズできる", func(t *testing.T) {
		// GIVEN
		r := Record{
			Lsn:   Lsn(2),
			TrxId: 10,
			Type:  RecordTypeCommit,
		}

		// WHEN
		buf := r.Serialize()

		// THEN
		assert.Equal(t, recordHeaderSize, len(buf))
	})

	t.Run("ROLLBACK レコードをシリアライズできる", func(t *testing.T) {
		// GIVEN
		r := Record{
			Lsn:   Lsn(3),
			TrxId: 10,
			Type:  RecordTypeRollback,
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
			Lsn:    Lsn(5),
			TrxId:  42,
			Type:   RecordTypePageWrite,
			PageId: page.NewPageId(page.FileId(1), page.PageNumber(10)),
			Data:   *pg,
		}
		buf := original.Serialize()

		// WHEN
		decoded, readBytes, err := DeserializeRecord(buf)

		// THEN
		assert.NoError(t, err)
		assert.Equal(t, len(buf), readBytes)
		assert.Equal(t, original.Lsn, decoded.Lsn)
		assert.Equal(t, original.TrxId, decoded.TrxId)
		assert.Equal(t, original.Type, decoded.Type)
		assert.Equal(t, original.PageId, decoded.PageId)
		assert.Equal(t, original.Data.ToBytes(), decoded.Data.ToBytes())
	})

	t.Run("COMMIT レコードのラウンドトリップ", func(t *testing.T) {
		// GIVEN
		original := Record{
			Lsn:   Lsn(6),
			TrxId: 42,
			Type:  RecordTypeCommit,
		}
		buf := original.Serialize()

		// WHEN
		decoded, readBytes, err := DeserializeRecord(buf)

		// THEN
		assert.NoError(t, err)
		assert.Equal(t, len(buf), readBytes)
		assert.Equal(t, original.Lsn, decoded.Lsn)
		assert.Equal(t, original.TrxId, decoded.TrxId)
		assert.Equal(t, original.Type, decoded.Type)
	})

	t.Run("ROLLBACK レコードのラウンドトリップ", func(t *testing.T) {
		// GIVEN
		original := Record{
			Lsn:   Lsn(7),
			TrxId: 42,
			Type:  RecordTypeRollback,
		}
		buf := original.Serialize()

		// WHEN
		decoded, readBytes, err := DeserializeRecord(buf)

		// THEN
		assert.NoError(t, err)
		assert.Equal(t, len(buf), readBytes)
		assert.Equal(t, original.Lsn, decoded.Lsn)
		assert.Equal(t, original.TrxId, decoded.TrxId)
		assert.Equal(t, original.Type, decoded.Type)
	})

	t.Run("ヘッダーサイズ未満のデータはエラーを返す", func(t *testing.T) {
		// GIVEN
		data := make([]byte, recordHeaderSize-1)

		// WHEN
		_, _, err := DeserializeRecord(data)

		// THEN
		assert.ErrorIs(t, err, ErrInvalidRecord)
	})

	t.Run("データ長が実データより大きい場合はエラーを返す", func(t *testing.T) {
		// GIVEN
		pg := buildTestPage(t)
		r := Record{
			Lsn:    Lsn(1),
			TrxId:  1,
			Type:   RecordTypePageWrite,
			PageId: page.NewPageId(page.FileId(1), page.PageNumber(1)),
			Data:   *pg,
		}
		buf := r.Serialize()
		// データ部分を切り詰めてデータ長と実データを不一致にする
		truncated := buf[:recordHeaderSize+10]

		// WHEN
		_, _, err := DeserializeRecord(truncated)

		// THEN
		assert.ErrorIs(t, err, ErrInvalidRecord)
	})

	t.Run("複数レコードが連続するバイト列から 1 件目を読み取れる", func(t *testing.T) {
		// GIVEN
		r1 := Record{Lsn: Lsn(1), TrxId: 1, Type: RecordTypeCommit}
		r2 := Record{Lsn: Lsn(2), TrxId: 2, Type: RecordTypeCommit}
		buf := append(r1.Serialize(), r2.Serialize()...)

		// WHEN
		decoded, readBytes, err := DeserializeRecord(buf)

		// THEN
		assert.NoError(t, err)
		assert.Equal(t, Lsn(1), decoded.Lsn)
		// readBytes で 2 件目の開始位置が分かる
		decoded2, _, err := DeserializeRecord(buf[readBytes:])
		assert.NoError(t, err)
		assert.Equal(t, Lsn(2), decoded2.Lsn)
	})
}

// buildTestPage はテスト用の 4KB ページを作成する
func buildTestPage(t *testing.T) *page.Page {
	t.Helper()
	data := make([]byte, page.PageSize)
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
