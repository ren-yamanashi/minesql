package slottedpage

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNewSlottedPage(t *testing.T) {
	t.Run("SlottedPage インスタンスが生成される", func(t *testing.T) {
		// GIVEN
		data := make([]byte, 128)

		// WHEN
		slottedPage := NewSlottedPage(data)

		// THEN
		assert.NotNil(t, slottedPage)
		assert.Equal(t, data, slottedPage.data)
	})

}

func TestCapacity(t *testing.T) {
	t.Run("Slotted Page の容量が正しく取得できる", func(t *testing.T) {
		// GIVEN
		data := make([]byte, 128)
		sp := NewSlottedPage(data)

		// WHEN
		capacity := sp.Capacity()

		// THEN
		assert.Equal(t, 120, capacity) // 128 - headerSize(8) = 120
	})
}

func TestNumSlots(t *testing.T) {
	t.Run("スロット数が正しく取得できる", func(t *testing.T) {
		// GIVEN
		data := make([]byte, 128)
		sp := NewSlottedPage(data)
		sp.Initialize()
		sp.Insert(0, 10)
		sp.Insert(1, 20)

		// WHEN
		numSlots := sp.NumSlots()

		// THEN
		assert.Equal(t, 2, numSlots)
	})
}

func TestPointerAt(t *testing.T) {
	t.Run("指定したインデックスのポインタが正しく取得できる", func(t *testing.T) {
		// GIVEN
		data := make([]byte, 128)
		sp := NewSlottedPage(data)
		sp.Initialize()
		sp.Insert(0, 10)

		// WHEN
		cellData := sp.pointerAt(0)

		// THEN
		assert.Equal(t, 10, int(cellData.size))
	})
}

func TestData(t *testing.T) {
	t.Run("指定したインデックスのデータが正しく取得できる", func(t *testing.T) {
		// GIVEN
		data := make([]byte, 128)
		sp := NewSlottedPage(data)
		sp.Initialize()
		sp.Insert(0, 10)
		cellData := sp.Data(0)
		testData := []byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}
		copy(cellData, testData)

		// WHEN
		retrievedData := sp.Data(0)

		// THEN
		assert.Equal(t, testData, retrievedData)
	})
}

func TestInitialize(t *testing.T) {
	t.Run("Slotted Page が正しく初期化される", func(t *testing.T) {
		// GIVEN
		data := make([]byte, 128)
		sp := NewSlottedPage(data)

		// WHEN
		sp.Initialize()

		// THEN
		assert.Equal(t, 0, sp.NumSlots())
		assert.Equal(t, 120, sp.FreeSpace()) // 128 - headerSize(8) = 120
	})
}

func TestSetPointer(t *testing.T) {
	t.Run("指定したインデックスのポインタが正しく設定できる", func(t *testing.T) {
		// GIVEN
		data := make([]byte, 128)
		sp := NewSlottedPage(data)
		sp.Initialize()

		// WHEN
		sp.setPointer(0, newPointer(100, 20))

		// THEN
		pointer := sp.pointerAt(0)
		assert.Equal(t, uint16(100), pointer.offset)
		assert.Equal(t, uint16(20), pointer.size)
	})
}

func TestInsertData(t *testing.T) {
	t.Run("データが正しく挿入できる (実際のデータ挿入ではなく、容量の確保が行われる)", func(t *testing.T) {
		// GIVEN
		data := make([]byte, 128)
		sp := NewSlottedPage(data)
		sp.Initialize()

		// WHEN
		success := sp.Insert(0, 20)

		// THEN
		assert.True(t, success)
		assert.Equal(t, 1, sp.NumSlots())
		assert.Equal(t, 96, sp.FreeSpace()) // 120 - 20 (data) - 4 (pointer) = 96
	})

	t.Run("空き容量が足りない場合、挿入に失敗する", func(t *testing.T) {
		// GIVEN
		data := make([]byte, 32)
		sp := NewSlottedPage(data)
		sp.Initialize()

		// WHEN
		success := sp.Insert(0, 100) // 空き領域 (24 bytes) より大きいサイズを挿入

		// THEN
		assert.False(t, success)
		assert.Equal(t, 0, sp.NumSlots())
	})
}

func TestResize(t *testing.T) {
	t.Run("正常にデータのリサイズができる", func(t *testing.T) {
		// GIVEN
		data := make([]byte, 256)
		sp := NewSlottedPage(data)
		sp.Initialize()

		sp.Insert(0, 20)
		initialFreeSpace := sp.FreeSpace()
		initialNumSlots := sp.NumSlots()

		// WHEN
		success := sp.Resize(0, 30) // 20 -> 30 に拡張

		// THEN
		assert.True(t, success)
		assert.Equal(t, initialNumSlots, sp.NumSlots())      // スロット数は変わらない
		assert.Equal(t, initialFreeSpace-10, sp.FreeSpace()) // 10 bytes 減少
	})

	t.Run("空き容量が足りない場合、リサイズに失敗する", func(t *testing.T) {
		// GIVEN
		data := make([]byte, 64)
		sp := NewSlottedPage(data)
		sp.Initialize()

		sp.Insert(0, 20)
		initialFreeSpace := sp.FreeSpace()

		// WHEN
		success := sp.Resize(0, 100) // 空き容量を超えるサイズに拡張

		// THEN
		assert.False(t, success)
		assert.Equal(t, initialFreeSpace, sp.FreeSpace()) // 空き容量は変わらない
	})
}

func TestRemove(t *testing.T) {
	t.Run("正常にデータの削除ができる", func(t *testing.T) {
		// GIVEN
		data := make([]byte, 128)
		sp := NewSlottedPage(data)
		sp.Initialize()
		sp.Insert(0, 20)
		sp.Insert(1, 30)
		sp.Insert(2, 10)
		initialNumSlots := sp.NumSlots()

		// WHEN
		sp.Remove(1)

		// THEN
		assert.Equal(t, initialNumSlots-1, sp.NumSlots())
		assert.Equal(t, 2, sp.NumSlots())
		assert.Equal(t, 10, len(sp.Data(1))) // 削除後、インデックス 1 は元のインデックス 2 のデータ (10 bytes) になる
	})
}
