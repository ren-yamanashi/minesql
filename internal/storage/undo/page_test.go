package undo

import (
	"testing"

	"github.com/ren-yamanashi/minesql/internal/storage/buffer"
	"github.com/ren-yamanashi/minesql/internal/storage/page"
	"github.com/ren-yamanashi/minesql/internal/storage/redo"
	"github.com/stretchr/testify/assert"
)

func TestNewPage(t *testing.T) {
	t.Run("buffer.Page から Undo ページを作成できる", func(t *testing.T) {
		// GIVEN
		bufPage := newTestBufferPage(t)

		// WHEN
		undoPage := NewPage(bufPage)

		// THEN
		assert.NotNil(t, undoPage)
	})
}

func TestCreatePage(t *testing.T) {
	t.Run("新規 Undo ページを作成できる", func(t *testing.T) {
		// GIVEN
		bufPage := newTestBufferPage(t)

		// WHEN
		undoPage := CreatePage(bufPage)

		// THEN
		assert.NotNil(t, undoPage)
	})

	t.Run("作成時にヘッダーが初期化される", func(t *testing.T) {
		// GIVEN
		bufPage := newTestBufferPage(t)

		// WHEN
		undoPage := CreatePage(bufPage)

		// THEN
		assert.Equal(t, uint16(0), undoPage.UsedBytes())
		assert.Equal(t, page.PageNumber(0), undoPage.NextPageNumber())
	})

	t.Run("既存のヘッダー値が上書きされる", func(t *testing.T) {
		// GIVEN: 既存ヘッダーに非ゼロ値が入っているページ
		bufPage := newTestBufferPage(t)
		existing := NewPage(bufPage)
		existing.initialize()
		_ = existing.append(make([]byte, 100))
		existing.setNextPageNumber(page.PageNumber(42))
		assert.NotEqual(t, uint16(0), existing.UsedBytes())
		assert.Equal(t, page.PageNumber(42), existing.NextPageNumber())

		// WHEN
		undoPage := CreatePage(bufPage)

		// THEN: ヘッダーがゼロクリアされる
		assert.Equal(t, uint16(0), undoPage.UsedBytes())
		assert.Equal(t, page.PageNumber(0), undoPage.NextPageNumber())
	})
}

func TestPageRecord(t *testing.T) {
	t.Run("Append したレコードを読み取れる", func(t *testing.T) {
		// GIVEN
		undoPage := newTestUndoPage(t)
		undoPage.initialize()
		f := &Fields{
			trxId:       1,
			undoNumber:  0,
			recordType:  RecordTypeInsert,
			prevRollPtr: NullPointer(),
			tableFileId: 1,
			columnSets:  [][][]byte{{[]byte("data")}},
		}
		serialized := f.Serialize()
		_ = undoPage.append(serialized)

		// WHEN
		result := undoPage.Record(0)

		// THEN
		assert.NotNil(t, result)
		assert.Equal(t, serialized, result)
	})

	t.Run("offset がボディサイズ以上の場合 nil を返す", func(t *testing.T) {
		// GIVEN
		undoPage := newTestUndoPage(t)
		undoPage.initialize()

		// WHEN
		result := undoPage.Record(len(undoPage.body))

		// THEN
		assert.Nil(t, result)
	})

	t.Run("offset からヘッダーを読み取れない場合 nil を返す", func(t *testing.T) {
		// GIVEN
		undoPage := newTestUndoPage(t)
		undoPage.initialize()
		offset := len(undoPage.body) - recordHeaderSize + 1

		// WHEN
		result := undoPage.Record(offset)

		// THEN
		assert.Nil(t, result)
	})

	t.Run("offset が 0 でないレコードを読み取れる", func(t *testing.T) {
		// GIVEN
		undoPage := newTestUndoPage(t)
		undoPage.initialize()
		f1 := &Fields{
			trxId: 1, undoNumber: 0, recordType: RecordTypeInsert,
			prevRollPtr: NullPointer(), tableFileId: 1,
			columnSets: [][][]byte{{[]byte("first")}},
		}
		f2 := &Fields{
			trxId: 2, undoNumber: 1, recordType: RecordTypeDelete,
			prevRollPtr: NullPointer(), tableFileId: 1,
			columnSets: [][][]byte{{[]byte("second")}},
		}
		s1 := f1.Serialize()
		s2 := f2.Serialize()
		_ = undoPage.append(s1)
		_ = undoPage.append(s2)

		// WHEN
		result := undoPage.Record(len(s1))

		// THEN
		assert.NotNil(t, result)
		assert.Equal(t, s2, result)
	})

	t.Run("dataLen がボディの残りサイズを超える場合 nil を返す", func(t *testing.T) {
		// GIVEN
		undoPage := newTestUndoPage(t)
		undoPage.initialize()
		f := &Fields{
			trxId:       1,
			undoNumber:  0,
			recordType:  RecordTypeInsert,
			prevRollPtr: NullPointer(),
			tableFileId: 1,
			columnSets:  [][][]byte{{make([]byte, len(undoPage.body))}},
		}
		serialized := f.Serialize()
		copy(undoPage.body, serialized[:recordHeaderSize])

		// WHEN
		result := undoPage.Record(0)

		// THEN
		assert.Nil(t, result)
	})
}

func TestPageUsedBytes(t *testing.T) {
	t.Run("初期化後は 0 を返す", func(t *testing.T) {
		// GIVEN
		undoPage := newTestUndoPage(t)
		undoPage.initialize()

		// WHEN
		used := undoPage.UsedBytes()

		// THEN
		assert.Equal(t, uint16(0), used)
	})

	t.Run("レコード追加後に使用量が増える", func(t *testing.T) {
		// GIVEN
		undoPage := newTestUndoPage(t)
		undoPage.initialize()
		_ = undoPage.append([]byte{0x01, 0x02, 0x03})

		// WHEN
		used := undoPage.UsedBytes()

		// THEN
		assert.Equal(t, uint16(3), used)
	})
}

func TestPageNextPageNumber(t *testing.T) {
	t.Run("初期化後は 0 を返す", func(t *testing.T) {
		// GIVEN
		undoPage := newTestUndoPage(t)
		undoPage.initialize()

		// WHEN
		next := undoPage.NextPageNumber()

		// THEN
		assert.Equal(t, page.PageNumber(0), next)
	})

	t.Run("設定した値を返す", func(t *testing.T) {
		// GIVEN
		undoPage := newTestUndoPage(t)
		undoPage.initialize()
		undoPage.setNextPageNumber(page.PageNumber(42))

		// WHEN
		next := undoPage.NextPageNumber()

		// THEN
		assert.Equal(t, page.PageNumber(42), next)
	})
}

func TestPageFreeSpace(t *testing.T) {
	t.Run("初期化後はボディ全体が空き", func(t *testing.T) {
		// GIVEN
		undoPage := newTestUndoPage(t)
		undoPage.initialize()
		bodySize := len(undoPage.body)

		// WHEN
		free := undoPage.FreeSpace()

		// THEN
		assert.Equal(t, bodySize, free)
	})

	t.Run("レコード追加後に空きが減る", func(t *testing.T) {
		// GIVEN
		undoPage := newTestUndoPage(t)
		undoPage.initialize()
		bodySize := len(undoPage.body)
		_ = undoPage.append([]byte{0x01, 0x02, 0x03})

		// WHEN
		free := undoPage.FreeSpace()

		// THEN
		assert.Equal(t, bodySize-3, free)
	})

	t.Run("ボディが満杯の場合 0 を返す", func(t *testing.T) {
		// GIVEN
		undoPage := newTestUndoPage(t)
		undoPage.initialize()
		_ = undoPage.append(make([]byte, len(undoPage.body)))

		// WHEN
		free := undoPage.FreeSpace()

		// THEN
		assert.Equal(t, 0, free)
	})
}

func TestPageInitialize(t *testing.T) {
	t.Run("UsedBytes と NextPageNumber が 0 に初期化される", func(t *testing.T) {
		// GIVEN
		undoPage := newTestUndoPage(t)

		// WHEN
		undoPage.initialize()

		// THEN
		assert.Equal(t, uint16(0), undoPage.UsedBytes())
		assert.Equal(t, page.PageNumber(0), undoPage.NextPageNumber())
	})

	t.Run("既存データがある状態でも初期化できる", func(t *testing.T) {
		// GIVEN
		undoPage := newTestUndoPage(t)
		undoPage.initialize()
		_ = undoPage.append([]byte{0x01, 0x02, 0x03})
		undoPage.setNextPageNumber(page.PageNumber(10))

		// WHEN
		undoPage.initialize()

		// THEN
		assert.Equal(t, uint16(0), undoPage.UsedBytes())
		assert.Equal(t, page.PageNumber(0), undoPage.NextPageNumber())
	})
}

func TestPageAppend(t *testing.T) {
	t.Run("レコードを追加できる", func(t *testing.T) {
		// GIVEN
		undoPage := newTestUndoPage(t)
		undoPage.initialize()
		record := []byte{0x01, 0x02, 0x03}

		// WHEN
		ok := undoPage.append(record)

		// THEN
		assert.True(t, ok)
		assert.Equal(t, uint16(3), undoPage.UsedBytes())
	})

	t.Run("複数回追加すると UsedBytes が累積する", func(t *testing.T) {
		// GIVEN
		undoPage := newTestUndoPage(t)
		undoPage.initialize()

		// WHEN
		ok1 := undoPage.append([]byte{0x01, 0x02})
		ok2 := undoPage.append([]byte{0x03, 0x04, 0x05})

		// THEN
		assert.True(t, ok1)
		assert.True(t, ok2)
		assert.Equal(t, uint16(5), undoPage.UsedBytes())
	})

	t.Run("追加成功時に bufPage の modifyCount が進む", func(t *testing.T) {
		// GIVEN
		undoPage := newTestUndoPage(t)
		undoPage.initialize()
		before := undoPage.bufPage.ModifyCount()

		// WHEN
		ok := undoPage.append([]byte{0x01, 0x02, 0x03})

		// THEN
		assert.True(t, ok)
		assert.Greater(t, undoPage.bufPage.ModifyCount(), before)
	})

	t.Run("空き不足の場合 false を返す", func(t *testing.T) {
		// GIVEN
		undoPage := newTestUndoPage(t)
		undoPage.initialize()
		bodySize := len(undoPage.body)
		largeRecord := make([]byte, bodySize+1)

		// WHEN
		ok := undoPage.append(largeRecord)

		// THEN
		assert.False(t, ok)
		assert.Equal(t, uint16(0), undoPage.UsedBytes())
	})

	t.Run("ボディサイズちょうどのレコードを追加できる", func(t *testing.T) {
		// GIVEN
		undoPage := newTestUndoPage(t)
		undoPage.initialize()
		bodySize := len(undoPage.body)
		record := make([]byte, bodySize)

		// WHEN
		ok := undoPage.append(record)

		// THEN
		assert.True(t, ok)
		assert.Equal(t, uint16(bodySize), undoPage.UsedBytes())
	})

	t.Run("ボディが満杯の場合 false を返す", func(t *testing.T) {
		// GIVEN
		undoPage := newTestUndoPage(t)
		undoPage.initialize()
		bodySize := len(undoPage.body)
		_ = undoPage.append(make([]byte, bodySize))

		// WHEN
		ok := undoPage.append([]byte{0x01})

		// THEN
		assert.False(t, ok)
	})

	t.Run("空のレコードを追加できる", func(t *testing.T) {
		// GIVEN
		undoPage := newTestUndoPage(t)
		undoPage.initialize()

		// WHEN
		ok := undoPage.append([]byte{})

		// THEN
		assert.True(t, ok)
		assert.Equal(t, uint16(0), undoPage.UsedBytes())
	})
}

func TestPageSetNextPageNumber(t *testing.T) {
	t.Run("次のページ番号を設定できる", func(t *testing.T) {
		// GIVEN
		undoPage := newTestUndoPage(t)
		undoPage.initialize()

		// WHEN
		undoPage.setNextPageNumber(page.PageNumber(42))

		// THEN
		assert.Equal(t, page.PageNumber(42), undoPage.NextPageNumber())
	})

	t.Run("設定した値を上書きできる", func(t *testing.T) {
		// GIVEN
		undoPage := newTestUndoPage(t)
		undoPage.initialize()
		undoPage.setNextPageNumber(page.PageNumber(10))

		// WHEN
		undoPage.setNextPageNumber(page.PageNumber(20))

		// THEN
		assert.Equal(t, page.PageNumber(20), undoPage.NextPageNumber())
	})
}

// newTestBufferPage はテスト用の buffer.Page を作成する
func newTestBufferPage(t *testing.T) *buffer.Page {
	t.Helper()
	rl, err := redo.NewBuffer(t.TempDir())
	if err != nil {
		t.Fatalf("redo.Buffer の作成に失敗: %v", err)
	}
	t.Cleanup(func() { _ = rl.Close() })
	pool := buffer.NewPool(page.Size, rl, nil)
	bufPage, err := pool.AddPage(page.NewId(0, 0))
	if err != nil {
		t.Fatalf("buffer.Page の作成に失敗: %v", err)
	}
	return bufPage
}

// newTestUndoPage はテスト用の初期化済み Undo Page を作成する
func newTestUndoPage(t *testing.T) *Page {
	t.Helper()
	bufPage := newTestBufferPage(t)
	return NewPage(bufPage)
}
