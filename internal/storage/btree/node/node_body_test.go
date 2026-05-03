package node

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNodeBodyInsert(t *testing.T) {
	t.Run("レコードを挿入できる", func(t *testing.T) {
		// GIVEN
		nb := newTestNodeBody()
		record := NewRecord([]byte{0x01}, []byte{0x10}, []byte{0xAA})

		// WHEN
		ok := nb.Insert(0, record)

		// THEN
		assert.True(t, ok)
		assert.Equal(t, 1, nb.NumRecords())
	})

	t.Run("maxRecordSize を超えるレコードは挿入できない", func(t *testing.T) {
		// GIVEN
		nb := newTestNodeBody()
		maxSize := nb.maxRecordSize()
		largeData := make([]byte, maxSize) // ToBytes で 4 バイト追加されるため超過する

		// WHEN
		ok := nb.Insert(0, NewRecord([]byte{}, []byte{}, largeData))

		// THEN
		assert.False(t, ok)
		assert.Equal(t, 0, nb.NumRecords())
	})
}

func TestNodeBodyDelete(t *testing.T) {
	t.Run("レコードを削除できる", func(t *testing.T) {
		// GIVEN
		nb := newTestNodeBody()
		nb.Insert(0, NewRecord([]byte{0x01}, []byte{0x10}, []byte{0xAA}))

		// WHEN
		nb.Delete(0)

		// THEN
		assert.Equal(t, 0, nb.NumRecords())
	})
}

func TestNodeBodyUpdate(t *testing.T) {
	t.Run("レコードを更新できる", func(t *testing.T) {
		// GIVEN
		nb := newTestNodeBody()
		nb.Insert(0, NewRecord([]byte{0x01}, []byte{0x10}, []byte{0xAA}))
		updated := NewRecord([]byte{0x02}, []byte{0x10}, []byte{0xBB})

		// WHEN
		ok := nb.Update(0, updated)

		// THEN
		assert.True(t, ok)
		assert.Equal(t, []byte{0xBB}, nb.Record(0).NonKey())
	})
}

func TestNodeBodyNumRecords(t *testing.T) {
	t.Run("レコード数を返す", func(t *testing.T) {
		// GIVEN
		nb := newTestNodeBody()
		nb.Insert(0, NewRecord([]byte{0x01}, []byte{0x10}, []byte{}))
		nb.Insert(1, NewRecord([]byte{0x01}, []byte{0x20}, []byte{}))

		// WHEN / THEN
		assert.Equal(t, 2, nb.NumRecords())
	})
}

func TestNodeBodyRecord(t *testing.T) {
	t.Run("挿入したレコードを取得できる", func(t *testing.T) {
		// GIVEN
		nb := newTestNodeBody()
		nb.Insert(0, NewRecord([]byte{0x01}, []byte{0x10}, []byte{0xAA}))

		// WHEN
		record := nb.Record(0)

		// THEN
		assert.Equal(t, []byte{0x01}, record.Header())
		assert.Equal(t, []byte{0x10}, record.Key())
		assert.Equal(t, []byte{0xAA}, record.NonKey())
	})
}

func TestNodeBodySearchSlotNum(t *testing.T) {
	t.Run("存在するキーのスロット番号を返す", func(t *testing.T) {
		// GIVEN
		nb := newTestNodeBody()
		nb.Insert(0, NewRecord([]byte{0x01}, []byte{0x10}, []byte{0xAA}))
		nb.Insert(1, NewRecord([]byte{0x01}, []byte{0x20}, []byte{0xBB}))

		// WHEN
		slotNum, found := nb.SearchSlotNum([]byte{0x20})

		// THEN
		assert.True(t, found)
		assert.Equal(t, 1, slotNum)
	})

	t.Run("存在しないキーは見つからない", func(t *testing.T) {
		// GIVEN
		nb := newTestNodeBody()
		nb.Insert(0, NewRecord([]byte{0x01}, []byte{0x10}, []byte{0xAA}))

		// WHEN
		_, found := nb.SearchSlotNum([]byte{0xFF})

		// THEN
		assert.False(t, found)
	})
}

func TestNodeBodyCanTransferRecord(t *testing.T) {
	t.Run("レコードが 1 つの場合は転送できない", func(t *testing.T) {
		// GIVEN
		nb := newTestNodeBody()
		nb.Insert(0, NewRecord([]byte{0x01}, []byte{0x10}, []byte{0xAA}))

		// WHEN/THEN
		assert.False(t, nb.CanTransferRecord(true))
		assert.False(t, nb.CanTransferRecord(false))
	})

	t.Run("転送後も半分以上埋まっている場合は true を返す", func(t *testing.T) {
		// GIVEN
		nb := newTestNodeBody()
		padding := make([]byte, 200)
		for i := range 15 {
			nb.Insert(i, NewRecord([]byte{0x01}, []byte{byte(i)}, padding))
		}

		// WHEN / THEN
		assert.True(t, nb.CanTransferRecord(true))
		assert.True(t, nb.CanTransferRecord(false))
	})
}

func TestNodeBodyIsHalfFull(t *testing.T) {
	t.Run("レコードが空の場合は半分以上埋まっていない", func(t *testing.T) {
		// GIVEN
		nb := newTestNodeBody()

		// WHEN/THEN
		assert.False(t, nb.IsHalfFull())
	})

	t.Run("半分以上埋まっている場合は true を返す", func(t *testing.T) {
		// GIVEN
		nb := newTestNodeBody()
		padding := make([]byte, 200)
		for i := range 15 {
			nb.Insert(i, NewRecord([]byte{0x01}, []byte{byte(i)}, padding))
		}

		// WHEN / THEN
		assert.True(t, nb.IsHalfFull())
	})
}

func TestNodeBodyTransfer(t *testing.T) {
	t.Run("先頭レコードを別のノードに移動できる", func(t *testing.T) {
		// GIVEN
		src := newTestNodeBody()
		src.Insert(0, NewRecord([]byte{0x01}, []byte{0x10}, []byte{0xAA}))
		src.Insert(1, NewRecord([]byte{0x01}, []byte{0x20}, []byte{0xBB}))
		dest := newTestNodeBody()

		// WHEN
		err := src.transfer(dest)

		// THEN
		assert.NoError(t, err)
		assert.Equal(t, 1, src.NumRecords())
		assert.Equal(t, 1, dest.NumRecords())
		assert.Equal(t, []byte{0x10}, dest.Record(0).Key())
		assert.Equal(t, []byte{0x20}, src.Record(0).Key())
	})
}

// newTestNodeBody は初期化済みの nodeBody を作成する
func newTestNodeBody() *nodeBody {
	data := make([]byte, 4096)
	sp := NewSlottedPage(data)
	sp.Initialize()
	return &nodeBody{body: sp}
}
