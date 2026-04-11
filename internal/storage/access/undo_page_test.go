package access

import (
	"encoding/binary"
	"testing"

	"minesql/internal/storage/page"

	"github.com/stretchr/testify/assert"
)

func TestNewUndoPage(t *testing.T) {
	t.Run("UndoPage インスタンスが生成される", func(t *testing.T) {
		// GIVEN
		data := make([]byte, 4096)

		// WHEN
		p := NewUndoPage(page.NewPage(data))

		// THEN
		assert.NotNil(t, p)
	})
}

func TestUndoPageInitialize(t *testing.T) {
	t.Run("Initialize 後の各フィールドがゼロ値になる", func(t *testing.T) {
		// GIVEN
		data := make([]byte, 4096)
		p := NewUndoPage(page.NewPage(data))

		// WHEN
		p.Initialize()

		// THEN
		assert.Equal(t, uint16(0), p.UsedBytes())
		assert.Equal(t, uint16(0), p.NextPageNumber())
		assert.Equal(t, 4096-page.PageHeaderSize-undoPageHeaderSize, p.FreeSpace())
	})
}

func TestUndoPageAppend(t *testing.T) {
	t.Run("レコードを追加できる", func(t *testing.T) {
		// GIVEN
		data := make([]byte, 4096)
		p := NewUndoPage(page.NewPage(data))
		p.Initialize()
		record := makeTestUndoRecord(1, 0, 1, []byte("hello"))

		// WHEN
		ok := p.Append(record)

		// THEN
		assert.True(t, ok)
		assert.Equal(t, uint16(len(record)), p.UsedBytes())
	})

	t.Run("複数レコードを追加できる", func(t *testing.T) {
		// GIVEN
		data := make([]byte, 4096)
		p := NewUndoPage(page.NewPage(data))
		p.Initialize()
		r1 := makeTestUndoRecord(1, 0, 1, []byte("aaa"))
		r2 := makeTestUndoRecord(1, 1, 2, []byte("bbb"))

		// WHEN
		ok1 := p.Append(r1)
		ok2 := p.Append(r2)

		// THEN
		assert.True(t, ok1)
		assert.True(t, ok2)
		assert.Equal(t, uint16(len(r1)+len(r2)), p.UsedBytes())
	})

	t.Run("空き不足の場合は false を返す", func(t *testing.T) {
		// GIVEN: 小さいページ (64 バイト)
		data := make([]byte, 64)
		p := NewUndoPage(page.NewPage(data))
		p.Initialize()
		// 64 - 4 (Page ヘッダー) - 4 (UNDO ヘッダー) = 56 バイトの空き
		largeRecord := makeTestUndoRecord(1, 0, 1, make([]byte, 50))

		// WHEN
		ok := p.Append(largeRecord)

		// THEN
		assert.False(t, ok)
	})
}

func TestUndoPageRecordAt(t *testing.T) {
	t.Run("追加したレコードを読み取れる", func(t *testing.T) {
		// GIVEN
		data := make([]byte, 4096)
		p := NewUndoPage(page.NewPage(data))
		p.Initialize()
		record := makeTestUndoRecord(42, 0, 1, []byte("hello"))
		p.Append(record)

		// WHEN
		result := p.RecordAt(0)

		// THEN
		assert.Equal(t, record, result)
	})

	t.Run("2 番目のレコードを offset 指定で読み取れる", func(t *testing.T) {
		// GIVEN
		data := make([]byte, 4096)
		p := NewUndoPage(page.NewPage(data))
		p.Initialize()
		r1 := makeTestUndoRecord(1, 0, 1, []byte("aaa"))
		r2 := makeTestUndoRecord(2, 1, 2, []byte("bbbbb"))
		p.Append(r1)
		p.Append(r2)

		// WHEN
		result := p.RecordAt(len(r1))

		// THEN
		assert.Equal(t, r2, result)
	})

	t.Run("範囲外の offset は nil を返す", func(t *testing.T) {
		// GIVEN
		data := make([]byte, 4096)
		p := NewUndoPage(page.NewPage(data))
		p.Initialize()

		// WHEN
		result := p.RecordAt(5000)

		// THEN
		assert.Nil(t, result)
	})
}

func TestUndoPageUsedBytes(t *testing.T) {
	t.Run("UsedBytes は追加したレコードの合計サイズを返す", func(t *testing.T) {
		// GIVEN
		data := make([]byte, 4096)
		p := NewUndoPage(page.NewPage(data))
		p.Initialize()
		r1 := makeTestUndoRecord(1, 0, 1, []byte("aaa"))
		r2 := makeTestUndoRecord(2, 1, 2, []byte("bbbbb"))
		p.Append(r1)
		p.Append(r2)

		// WHEN
		used := p.UsedBytes()

		// THEN
		assert.Equal(t, uint16(len(r1)+len(r2)), used)
	})
}

func TestUndoPageNextPageNumber(t *testing.T) {
	t.Run("SetNextPageNumber で設定した値を NextPageNumber で読み取れる", func(t *testing.T) {
		// GIVEN
		data := make([]byte, 4096)
		p := NewUndoPage(page.NewPage(data))
		p.Initialize()

		// WHEN
		p.SetNextPageNumber(42)

		// THEN
		assert.Equal(t, uint16(42), p.NextPageNumber())
	})
}

func TestUndoPageFreeSpace(t *testing.T) {
	t.Run("レコード追加後に空き容量が減る", func(t *testing.T) {
		// GIVEN
		data := make([]byte, 4096)
		p := NewUndoPage(page.NewPage(data))
		p.Initialize()
		initialFree := p.FreeSpace()
		record := makeTestUndoRecord(1, 0, 1, []byte("hello"))

		// WHEN
		p.Append(record)

		// THEN
		assert.Equal(t, initialFree-len(record), p.FreeSpace())
	})
}

// テスト用の UNDO レコードを作成する (TrxId + UndoNo + Type + DataLen + Data)
func makeTestUndoRecord(trxId uint64, undoNo uint64, recordType uint8, data []byte) []byte {
	buf := make([]byte, 19+len(data))
	binary.BigEndian.PutUint64(buf[0:8], trxId)
	binary.BigEndian.PutUint64(buf[8:16], undoNo)
	buf[16] = recordType
	binary.BigEndian.PutUint16(buf[17:19], uint16(len(data)))
	copy(buf[19:], data)
	return buf
}
