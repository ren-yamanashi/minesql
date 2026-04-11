package log

import (
	"minesql/internal/storage/page"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSerialize(t *testing.T) {
	t.Run("ページ変更レコードをシリアライズできる", func(t *testing.T) {
		// GIVEN
		data := make([]byte, 4096)
		data[0] = 0xAA
		record := RedoRecord{
			LSN:    1,
			TrxId:  100,
			Type:   RedoPageWrite,
			PageId: page.NewPageId(1, 2),
			Data:   data,
		}

		// WHEN
		buf := record.Serialize()

		// THEN
		assert.Equal(t, redoRecordHeaderSize+4096, len(buf))
	})

	t.Run("COMMIT レコードをシリアライズできる", func(t *testing.T) {
		// GIVEN
		record := RedoRecord{
			LSN:   2,
			TrxId: 100,
			Type:  RedoCommit,
		}

		// WHEN
		buf := record.Serialize()

		// THEN
		assert.Equal(t, redoRecordHeaderSize, len(buf))
	})

	t.Run("ROLLBACK レコードをシリアライズできる", func(t *testing.T) {
		// GIVEN
		record := RedoRecord{
			LSN:   3,
			TrxId: 100,
			Type:  RedoRollback,
		}

		// WHEN
		buf := record.Serialize()

		// THEN
		assert.Equal(t, redoRecordHeaderSize, len(buf))
	})
}

func TestDeserializeRedoRecord(t *testing.T) {
	t.Run("ページ変更レコードをデシリアライズできる", func(t *testing.T) {
		// GIVEN
		data := make([]byte, 4096)
		data[0] = 0xAA
		data[4095] = 0xBB
		original := RedoRecord{
			LSN:    42,
			TrxId:  100,
			Type:   RedoPageWrite,
			PageId: page.NewPageId(1, 2),
			Data:   data,
		}
		buf := original.Serialize()

		// WHEN
		record, bytesRead, err := DeserializeRedoRecord(buf)

		// THEN
		assert.NoError(t, err)
		assert.Equal(t, redoRecordHeaderSize+4096, bytesRead)
		assert.Equal(t, LSN(42), record.LSN)
		assert.Equal(t, uint64(100), record.TrxId)
		assert.Equal(t, RedoPageWrite, record.Type)
		assert.Equal(t, page.NewPageId(1, 2), record.PageId)
		assert.Equal(t, byte(0xAA), record.Data[0])
		assert.Equal(t, byte(0xBB), record.Data[4095])
	})

	t.Run("COMMIT レコードをデシリアライズできる", func(t *testing.T) {
		// GIVEN
		original := RedoRecord{
			LSN:   10,
			TrxId: 200,
			Type:  RedoCommit,
		}
		buf := original.Serialize()

		// WHEN
		record, bytesRead, err := DeserializeRedoRecord(buf)

		// THEN
		assert.NoError(t, err)
		assert.Equal(t, redoRecordHeaderSize, bytesRead)
		assert.Equal(t, LSN(10), record.LSN)
		assert.Equal(t, uint64(200), record.TrxId)
		assert.Equal(t, RedoCommit, record.Type)
		assert.Nil(t, record.Data)
	})

	t.Run("連続するレコードを順次デシリアライズできる", func(t *testing.T) {
		// GIVEN
		r1 := RedoRecord{LSN: 1, TrxId: 1, Type: RedoCommit}
		r2 := RedoRecord{LSN: 2, TrxId: 2, Type: RedoRollback}
		buf := append(r1.Serialize(), r2.Serialize()...)

		// WHEN
		rec1, n1, err1 := DeserializeRedoRecord(buf)
		rec2, n2, err2 := DeserializeRedoRecord(buf[n1:])

		// THEN
		assert.NoError(t, err1)
		assert.NoError(t, err2)
		assert.Equal(t, LSN(1), rec1.LSN)
		assert.Equal(t, RedoCommit, rec1.Type)
		assert.Equal(t, LSN(2), rec2.LSN)
		assert.Equal(t, RedoRollback, rec2.Type)
		assert.Equal(t, len(buf), n1+n2)
	})

	t.Run("データが不足している場合はエラーを返す", func(t *testing.T) {
		// GIVEN
		buf := make([]byte, redoRecordHeaderSize-1)

		// WHEN
		_, _, err := DeserializeRedoRecord(buf)

		// THEN
		assert.ErrorIs(t, err, ErrInvalidRedoRecord)
	})

	t.Run("DataLen に対してデータが不足している場合はエラーを返す", func(t *testing.T) {
		// GIVEN
		record := RedoRecord{
			LSN:    1,
			TrxId:  1,
			Type:   RedoPageWrite,
			PageId: page.NewPageId(1, 0),
			Data:   make([]byte, 4096),
		}
		buf := record.Serialize()
		truncated := buf[:redoRecordHeaderSize+100] // データを途中で切る

		// WHEN
		_, _, err := DeserializeRedoRecord(truncated)

		// THEN
		assert.ErrorIs(t, err, ErrInvalidRedoRecord)
	})
}
