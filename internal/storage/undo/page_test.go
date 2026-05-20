package undo

import (
	"testing"

	"github.com/ren-yamanashi/minesql/internal/storage/page"
	"github.com/stretchr/testify/assert"
)

func TestNewPage(t *testing.T) {
	t.Run("page.Page から Undo ページを作成できる", func(t *testing.T) {
		// GIVEN
		pg := newTestPage(t)

		// WHEN
		undoPage := NewPage(*pg)

		// THEN
		assert.NotNil(t, undoPage)
	})
}

func TestInitialize(t *testing.T) {
	t.Run("UsedBytes と NextPageNumber が 0 に初期化される", func(t *testing.T) {
		// GIVEN
		undoPage := newTestUndoPage(t)

		// WHEN
		undoPage.Initialize()

		// THEN
		assert.Equal(t, uint16(0), undoPage.UsedBytes())
		assert.Equal(t, page.PageNumber(0), undoPage.NextPageNumber())
	})
}

func TestAppend(t *testing.T) {
	t.Run("レコードを追加できる", func(t *testing.T) {
		// GIVEN
		undoPage := newTestUndoPage(t)
		undoPage.Initialize()
		record := []byte{0x01, 0x02, 0x03}

		// WHEN
		ok := undoPage.Append(record)

		// THEN
		assert.True(t, ok)
		assert.Equal(t, uint16(3), undoPage.UsedBytes())
	})

	t.Run("複数回追加すると UsedBytes が累積する", func(t *testing.T) {
		// GIVEN
		undoPage := newTestUndoPage(t)
		undoPage.Initialize()

		// WHEN
		ok1 := undoPage.Append([]byte{0x01, 0x02})
		ok2 := undoPage.Append([]byte{0x03, 0x04, 0x05})

		// THEN
		assert.True(t, ok1)
		assert.True(t, ok2)
		assert.Equal(t, uint16(5), undoPage.UsedBytes())
	})

	t.Run("空き不足の場合 false を返す", func(t *testing.T) {
		// GIVEN
		undoPage := newTestUndoPage(t)
		undoPage.Initialize()
		bodySize := len(undoPage.body)
		largeRecord := make([]byte, bodySize+1)

		// WHEN
		ok := undoPage.Append(largeRecord)

		// THEN
		assert.False(t, ok)
		assert.Equal(t, uint16(0), undoPage.UsedBytes())
	})

	t.Run("ボディサイズちょうどのレコードを追加できる", func(t *testing.T) {
		// GIVEN
		undoPage := newTestUndoPage(t)
		undoPage.Initialize()
		bodySize := len(undoPage.body)
		record := make([]byte, bodySize)

		// WHEN
		ok := undoPage.Append(record)

		// THEN
		assert.True(t, ok)
		assert.Equal(t, uint16(bodySize), undoPage.UsedBytes())
	})

	t.Run("ボディが満杯の場合 false を返す", func(t *testing.T) {
		// GIVEN
		undoPage := newTestUndoPage(t)
		undoPage.Initialize()
		bodySize := len(undoPage.body)
		_ = undoPage.Append(make([]byte, bodySize))

		// WHEN
		ok := undoPage.Append([]byte{0x01})

		// THEN
		assert.False(t, ok)
	})

	t.Run("空のレコードを追加できる", func(t *testing.T) {
		// GIVEN
		undoPage := newTestUndoPage(t)
		undoPage.Initialize()

		// WHEN
		ok := undoPage.Append([]byte{})

		// THEN
		assert.True(t, ok)
		assert.Equal(t, uint16(0), undoPage.UsedBytes())
	})
}

func TestUsedBytes(t *testing.T) {
	t.Run("初期化後は 0 を返す", func(t *testing.T) {
		// GIVEN
		undoPage := newTestUndoPage(t)
		undoPage.Initialize()

		// WHEN
		used := undoPage.UsedBytes()

		// THEN
		assert.Equal(t, uint16(0), used)
	})
}

func TestNextPageNumber(t *testing.T) {
	t.Run("初期化後は 0 を返す", func(t *testing.T) {
		// GIVEN
		undoPage := newTestUndoPage(t)
		undoPage.Initialize()

		// WHEN
		next := undoPage.NextPageNumber()

		// THEN
		assert.Equal(t, page.PageNumber(0), next)
	})
}

func TestSetNextPageNumber(t *testing.T) {
	t.Run("次のページ番号を設定できる", func(t *testing.T) {
		// GIVEN
		undoPage := newTestUndoPage(t)
		undoPage.Initialize()

		// WHEN
		undoPage.SetNextPageNumber(page.PageNumber(42))

		// THEN
		assert.Equal(t, page.PageNumber(42), undoPage.NextPageNumber())
	})

	t.Run("設定した値を上書きできる", func(t *testing.T) {
		// GIVEN
		undoPage := newTestUndoPage(t)
		undoPage.Initialize()
		undoPage.SetNextPageNumber(page.PageNumber(10))

		// WHEN
		undoPage.SetNextPageNumber(page.PageNumber(20))

		// THEN
		assert.Equal(t, page.PageNumber(20), undoPage.NextPageNumber())
	})
}

func TestFreeSpace(t *testing.T) {
	t.Run("初期化後はボディ全体が空き", func(t *testing.T) {
		// GIVEN
		undoPage := newTestUndoPage(t)
		undoPage.Initialize()
		bodySize := len(undoPage.body)

		// WHEN
		free := undoPage.FreeSpace()

		// THEN
		assert.Equal(t, bodySize, free)
	})

	t.Run("レコード追加後に空きが減る", func(t *testing.T) {
		// GIVEN
		undoPage := newTestUndoPage(t)
		undoPage.Initialize()
		bodySize := len(undoPage.body)
		_ = undoPage.Append([]byte{0x01, 0x02, 0x03})

		// WHEN
		free := undoPage.FreeSpace()

		// THEN
		assert.Equal(t, bodySize-3, free)
	})

	t.Run("ボディが満杯の場合 0 を返す", func(t *testing.T) {
		// GIVEN
		undoPage := newTestUndoPage(t)
		undoPage.Initialize()
		_ = undoPage.Append(make([]byte, len(undoPage.body)))

		// WHEN
		free := undoPage.FreeSpace()

		// THEN
		assert.Equal(t, 0, free)
	})
}

func TestRecordAt(t *testing.T) {
	t.Run("Append したレコードを読み取れる", func(t *testing.T) {
		// GIVEN
		undoPage := newTestUndoPage(t)
		undoPage.Initialize()
		f := &Fields{
			TrxId:       1,
			UndoNum:     0,
			RecordType:  RecordTypeInsert,
			PrevRollPtr: NullPointer,
			TableFileId: 1,
			ColumnSets:  [][][]byte{{[]byte("data")}},
		}
		serialized := f.Serialize()
		_ = undoPage.Append(serialized)

		// WHEN
		result := undoPage.RecordAt(0)

		// THEN
		assert.NotNil(t, result)
		assert.Equal(t, serialized, result)
	})

	t.Run("offset がボディサイズ以上の場合 nil を返す", func(t *testing.T) {
		// GIVEN
		undoPage := newTestUndoPage(t)
		undoPage.Initialize()

		// WHEN
		result := undoPage.RecordAt(len(undoPage.body))

		// THEN
		assert.Nil(t, result)
	})

	t.Run("offset からヘッダーを読み取れない場合 nil を返す", func(t *testing.T) {
		// GIVEN
		undoPage := newTestUndoPage(t)
		undoPage.Initialize()
		// ボディ末尾から recordHeaderSize 未満の位置
		offset := len(undoPage.body) - recordHeaderSize + 1

		// WHEN
		result := undoPage.RecordAt(offset)

		// THEN
		assert.Nil(t, result)
	})

	t.Run("offset が 0 でないレコードを読み取れる", func(t *testing.T) {
		// GIVEN
		undoPage := newTestUndoPage(t)
		undoPage.Initialize()
		f1 := &Fields{
			TrxId: 1, UndoNum: 0, RecordType: RecordTypeInsert,
			PrevRollPtr: NullPointer, TableFileId: 1,
			ColumnSets: [][][]byte{{[]byte("first")}},
		}
		f2 := &Fields{
			TrxId: 2, UndoNum: 1, RecordType: RecordTypeDelete,
			PrevRollPtr: NullPointer, TableFileId: 1,
			ColumnSets: [][][]byte{{[]byte("second")}},
		}
		s1 := f1.Serialize()
		s2 := f2.Serialize()
		_ = undoPage.Append(s1)
		_ = undoPage.Append(s2)

		// WHEN
		result := undoPage.RecordAt(len(s1))

		// THEN
		assert.NotNil(t, result)
		assert.Equal(t, s2, result)
	})

	t.Run("dataLen がボディの残りサイズを超える場合 nil を返す", func(t *testing.T) {
		// GIVEN
		undoPage := newTestUndoPage(t)
		undoPage.Initialize()
		// ボディ先頭にヘッダーだけ書き込み、dataLen をボディサイズより大きい値に設定
		// RecordAt は p.body[headerDataLenOffset:recordHeaderSize] から dataLen を読む (offset 非加算)
		f := &Fields{
			TrxId:       1,
			UndoNum:     0,
			RecordType:  RecordTypeInsert,
			PrevRollPtr: NullPointer,
			TableFileId: 1,
			ColumnSets:  [][][]byte{{make([]byte, len(undoPage.body))}},
		}
		serialized := f.Serialize()
		// ヘッダーだけコピー (本体は入りきらない)
		copy(undoPage.body, serialized[:recordHeaderSize])

		// WHEN
		result := undoPage.RecordAt(0)

		// THEN
		assert.Nil(t, result)
	})
}

// newTestPage はテスト用の page.Page を作成する
func newTestPage(t *testing.T) *page.Page {
	t.Helper()
	data := make([]byte, page.PageSize)
	pg, err := page.NewPage(data)
	if err != nil {
		t.Fatalf("page.Page の作成に失敗: %v", err)
	}
	return pg
}

// newTestUndoPage はテスト用の初期化済み Undo Page を作成する
func newTestUndoPage(t *testing.T) *Page {
	t.Helper()
	pg := newTestPage(t)
	return NewPage(*pg)
}
