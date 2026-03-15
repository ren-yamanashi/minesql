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
		sp.Insert(0, make([]byte, 10))
		sp.Insert(1, make([]byte, 20))

		// WHEN
		numSlots := sp.NumSlots()

		// THEN
		assert.Equal(t, 2, numSlots)
	})
}

func TestFreeSpace(t *testing.T) {
	t.Run("初期化直後は容量と等しい", func(t *testing.T) {
		// GIVEN
		data := make([]byte, 128)
		sp := NewSlottedPage(data)
		sp.Initialize()

		// WHEN
		freeSpace := sp.FreeSpace()

		// THEN
		assert.Equal(t, 120, freeSpace) // 128 - headerSize(8) = 120
	})

	t.Run("データ挿入後に空き容量が減る", func(t *testing.T) {
		// GIVEN
		data := make([]byte, 128)
		sp := NewSlottedPage(data)
		sp.Initialize()
		sp.Insert(0, make([]byte, 20))

		// WHEN
		freeSpace := sp.FreeSpace()

		// THEN: 120 - 20 (data) - 4 (pointer) = 96
		assert.Equal(t, 96, freeSpace)
	})

	t.Run("複数回挿入後に空き容量が正しく計算される", func(t *testing.T) {
		// GIVEN
		data := make([]byte, 256)
		sp := NewSlottedPage(data)
		sp.Initialize()
		sp.Insert(0, make([]byte, 10))
		sp.Insert(1, make([]byte, 30))
		sp.Insert(2, make([]byte, 20))

		// WHEN
		freeSpace := sp.FreeSpace()

		// THEN: 248 - (10 + 30 + 20) (data) - 3*4 (pointer) = 176
		assert.Equal(t, 176, freeSpace)
	})

	t.Run("削除後に空き容量が増える", func(t *testing.T) {
		// GIVEN
		data := make([]byte, 128)
		sp := NewSlottedPage(data)
		sp.Initialize()
		sp.Insert(0, make([]byte, 20))
		sp.Insert(1, make([]byte, 30))
		freeSpaceBefore := sp.FreeSpace()

		// WHEN
		sp.Remove(0)
		freeSpaceAfter := sp.FreeSpace()

		// THEN: 削除したデータ (20) + ポインタ (4) 分の空きが増える
		assert.Equal(t, freeSpaceBefore+20+4, freeSpaceAfter)
	})
}

func TestData(t *testing.T) {
	t.Run("指定したインデックスのデータが正しく取得できる", func(t *testing.T) {
		// GIVEN
		data := make([]byte, 128)
		sp := NewSlottedPage(data)
		sp.Initialize()
		testData := []byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}
		sp.Insert(0, testData)

		// WHEN
		retrievedData := sp.Data(0)

		// THEN
		assert.Equal(t, testData, retrievedData)
	})

	t.Run("複数スロットがある場合に各インデックスのデータ領域が独立している", func(t *testing.T) {
		// GIVEN
		data := make([]byte, 256)
		sp := NewSlottedPage(data)
		sp.Initialize()
		sp.Insert(0, []byte("AAAAA"))
		sp.Insert(1, []byte("BBBBB"))
		sp.Insert(2, []byte("CCCCC"))

		// WHEN: スロット 1 を書き換える
		copy(sp.Data(1), []byte("XXXXX"))

		// THEN: 他のスロットには影響しない
		assert.Equal(t, []byte("AAAAA"), sp.Data(0))
		assert.Equal(t, []byte("XXXXX"), sp.Data(1))
		assert.Equal(t, []byte("CCCCC"), sp.Data(2))
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

func TestSlottedPage_Insert(t *testing.T) {
	t.Run("データが正しく挿入できる", func(t *testing.T) {
		// GIVEN
		data := make([]byte, 128)
		sp := NewSlottedPage(data)
		sp.Initialize()

		// WHEN
		success := sp.Insert(0, make([]byte, 20))

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
		success := sp.Insert(0, make([]byte, 100)) // 空き領域 (24 bytes) より大きいサイズを挿入

		// THEN
		assert.False(t, success)
		assert.Equal(t, 0, sp.NumSlots())
	})

	t.Run("中間位置への挿入でポインタ配列が正しくシフトされる", func(t *testing.T) {
		// GIVEN
		data := make([]byte, 256)
		sp := NewSlottedPage(data)
		sp.Initialize()
		sp.Insert(0, []byte("AAAAA"))
		sp.Insert(1, []byte("BBBBB"))
		sp.Insert(2, []byte("CCCCC"))

		// WHEN: index=1 に新しいスロットを挿入
		success := sp.Insert(1, []byte("XXXXX"))

		// THEN
		assert.True(t, success)
		assert.Equal(t, 4, sp.NumSlots())
		assert.Equal(t, []byte("AAAAA"), sp.Data(0))
		assert.Equal(t, []byte("XXXXX"), sp.Data(1))
		assert.Equal(t, []byte("BBBBB"), sp.Data(2))
		assert.Equal(t, []byte("CCCCC"), sp.Data(3))
	})

	t.Run("複数回挿入後に各スロットのデータが正しく読み書きできる", func(t *testing.T) {
		// GIVEN
		data := make([]byte, 256)
		sp := NewSlottedPage(data)
		sp.Initialize()

		// WHEN
		sp.Insert(0, []byte("aaa"))
		sp.Insert(1, []byte("bbbb"))
		sp.Insert(2, []byte("ccccc"))

		// THEN
		assert.Equal(t, 3, sp.NumSlots())
		assert.Equal(t, []byte("aaa"), sp.Data(0))
		assert.Equal(t, []byte("bbbb"), sp.Data(1))
		assert.Equal(t, []byte("ccccc"), sp.Data(2))
	})

	t.Run("空き容量ちょうどのサイズを挿入できる", func(t *testing.T) {
		// GIVEN
		data := make([]byte, 32)
		sp := NewSlottedPage(data)
		sp.Initialize()
		// capacity = 32 - 8 = 24, FreeSpace = 24
		// 挿入に必要な容量: dataSize + pointerSize(4) = 24 → dataSize = 20

		// WHEN
		success := sp.Insert(0, make([]byte, 20))

		// THEN
		assert.True(t, success)
		assert.Equal(t, 0, sp.FreeSpace())
	})
}

func TestResize(t *testing.T) {
	t.Run("拡張方向のリサイズが正しく動作する", func(t *testing.T) {
		// GIVEN
		data := make([]byte, 256)
		sp := NewSlottedPage(data)
		sp.Initialize()

		sp.Insert(0, make([]byte, 20))
		initialFreeSpace := sp.FreeSpace()
		initialNumSlots := sp.NumSlots()

		// WHEN
		success := sp.Resize(0, 30) // 20 -> 30 に拡張

		// THEN
		assert.True(t, success)
		assert.Equal(t, initialNumSlots, sp.NumSlots())      // スロット数は変わらない
		assert.Equal(t, initialFreeSpace-10, sp.FreeSpace()) // 10 bytes 減少
		assert.Equal(t, 30, len(sp.Data(0)))                 // データ領域が 30 になる
	})

	t.Run("空き容量が足りない場合、リサイズに失敗する", func(t *testing.T) {
		// GIVEN
		data := make([]byte, 64)
		sp := NewSlottedPage(data)
		sp.Initialize()

		sp.Insert(0, make([]byte, 20))
		initialFreeSpace := sp.FreeSpace()

		// WHEN
		success := sp.Resize(0, 100) // 空き容量を超えるサイズに拡張

		// THEN
		assert.False(t, success)
		assert.Equal(t, initialFreeSpace, sp.FreeSpace()) // 空き容量は変わらない
	})

	t.Run("縮小方向のリサイズが正しく動作する", func(t *testing.T) {
		// GIVEN
		data := make([]byte, 256)
		sp := NewSlottedPage(data)
		sp.Initialize()
		sp.Insert(0, make([]byte, 20))
		initialFreeSpace := sp.FreeSpace()

		// WHEN
		success := sp.Resize(0, 10) // 20 -> 10 に縮小

		// THEN
		assert.True(t, success)
		assert.Equal(t, initialFreeSpace+10, sp.FreeSpace()) // 10 bytes 増加
		assert.Equal(t, 10, len(sp.Data(0)))                 // データ領域が 10 になる
	})

	t.Run("同じサイズへのリサイズは何も変更しない", func(t *testing.T) {
		// GIVEN
		data := make([]byte, 256)
		sp := NewSlottedPage(data)
		sp.Initialize()
		sp.Insert(0, make([]byte, 20))
		initialFreeSpace := sp.FreeSpace()

		// WHEN
		success := sp.Resize(0, 20) // 20 -> 20

		// THEN
		assert.True(t, success)
		assert.Equal(t, initialFreeSpace, sp.FreeSpace())
	})

	t.Run("リサイズ後もデータ内容が保持される", func(t *testing.T) {
		// GIVEN
		data := make([]byte, 256)
		sp := NewSlottedPage(data)
		sp.Initialize()
		sp.Insert(0, []byte("0123456789"))

		// WHEN: 10 -> 20 に拡張
		success := sp.Resize(0, 20)

		// THEN: 拡張時はデータ領域が低アドレス方向に伸びるため、元データは末尾側に残る
		assert.True(t, success)
		assert.Equal(t, 20, len(sp.Data(0)))
		assert.Equal(t, []byte("0123456789"), sp.Data(0)[10:])
	})

	t.Run("複数スロットがある場合のリサイズで他のスロットのデータが壊れない", func(t *testing.T) {
		// GIVEN
		data := make([]byte, 256)
		sp := NewSlottedPage(data)
		sp.Initialize()
		sp.Insert(0, []byte("AAAAA"))
		sp.Insert(1, []byte("BBBBB"))
		sp.Insert(2, []byte("CCCCC"))

		// WHEN: 中間のスロット (index=1) を拡張
		success := sp.Resize(1, 10)

		// THEN: 他のスロットのデータは壊れない
		assert.True(t, success)
		assert.Equal(t, []byte("AAAAA"), sp.Data(0))
		assert.Equal(t, 10, len(sp.Data(1)))
		assert.Equal(t, []byte("BBBBB"), sp.Data(1)[5:]) // 元データは末尾側に残る
		assert.Equal(t, []byte("CCCCC"), sp.Data(2))
	})
}

func TestRemove(t *testing.T) {
	t.Run("中間スロットの削除が正しく動作する", func(t *testing.T) {
		// GIVEN
		data := make([]byte, 256)
		sp := NewSlottedPage(data)
		sp.Initialize()
		sp.Insert(0, []byte("AAAAA"))
		sp.Insert(1, []byte("BBBBB"))
		sp.Insert(2, []byte("CCCCC"))

		// WHEN
		sp.Remove(1)

		// THEN
		assert.Equal(t, 2, sp.NumSlots())
		assert.Equal(t, []byte("AAAAA"), sp.Data(0))
		assert.Equal(t, []byte("CCCCC"), sp.Data(1))
	})

	t.Run("先頭スロットの削除が正しく動作する", func(t *testing.T) {
		// GIVEN
		data := make([]byte, 256)
		sp := NewSlottedPage(data)
		sp.Initialize()
		sp.Insert(0, []byte("AAAAA"))
		sp.Insert(1, []byte("BBBBB"))
		sp.Insert(2, []byte("CCCCC"))

		// WHEN
		sp.Remove(0)

		// THEN
		assert.Equal(t, 2, sp.NumSlots())
		assert.Equal(t, []byte("BBBBB"), sp.Data(0))
		assert.Equal(t, []byte("CCCCC"), sp.Data(1))
	})

	t.Run("末尾スロットの削除が正しく動作する", func(t *testing.T) {
		// GIVEN
		data := make([]byte, 256)
		sp := NewSlottedPage(data)
		sp.Initialize()
		sp.Insert(0, []byte("AAAAA"))
		sp.Insert(1, []byte("BBBBB"))
		sp.Insert(2, []byte("CCCCC"))

		// WHEN
		sp.Remove(2)

		// THEN
		assert.Equal(t, 2, sp.NumSlots())
		assert.Equal(t, []byte("AAAAA"), sp.Data(0))
		assert.Equal(t, []byte("BBBBB"), sp.Data(1))
	})

	t.Run("唯一のスロットを削除すると初期状態に戻る", func(t *testing.T) {
		// GIVEN
		data := make([]byte, 128)
		sp := NewSlottedPage(data)
		sp.Initialize()
		sp.Insert(0, make([]byte, 20))

		// WHEN
		sp.Remove(0)

		// THEN
		assert.Equal(t, 0, sp.NumSlots())
		assert.Equal(t, sp.Capacity(), sp.FreeSpace())
	})
}

func TestTransferAllTo(t *testing.T) {
	t.Run("すべてのスロットが転送先に移動し、転送元は空になる", func(t *testing.T) {
		// GIVEN
		srcData := make([]byte, 256)
		src := NewSlottedPage(srcData)
		src.Initialize()
		src.Insert(0, []byte("0123456789"))
		src.Insert(1, []byte("abcde"))

		destData := make([]byte, 256)
		dest := NewSlottedPage(destData)
		dest.Initialize()
		dest.Insert(0, []byte("existing"))

		// WHEN
		result := src.TransferAllTo(dest)

		// THEN
		assert.True(t, result)
		assert.Equal(t, 0, src.NumSlots())  // 転送元は空
		assert.Equal(t, 3, dest.NumSlots()) // 転送先は元の 1 + 転送された 2 = 3
		assert.Equal(t, []byte("existing"), dest.Data(0))
		assert.Equal(t, []byte("0123456789"), dest.Data(1))
		assert.Equal(t, []byte("abcde"), dest.Data(2))
	})

	t.Run("転送元が空の場合、何も転送されず true を返す", func(t *testing.T) {
		// GIVEN
		srcData := make([]byte, 128)
		src := NewSlottedPage(srcData)
		src.Initialize()

		destData := make([]byte, 128)
		dest := NewSlottedPage(destData)
		dest.Initialize()
		dest.Insert(0, make([]byte, 10))

		// WHEN
		result := src.TransferAllTo(dest)

		// THEN
		assert.True(t, result)
		assert.Equal(t, 0, src.NumSlots())
		assert.Equal(t, 1, dest.NumSlots())
	})

	t.Run("転送先の空き容量が不足している場合、false を返しデータが壊れない", func(t *testing.T) {
		// GIVEN
		srcData := make([]byte, 128)
		src := NewSlottedPage(srcData)
		src.Initialize()
		src.Insert(0, make([]byte, 50))

		destData := make([]byte, 32) // 小さいページ
		dest := NewSlottedPage(destData)
		dest.Initialize()

		// WHEN
		result := src.TransferAllTo(dest)

		// THEN
		assert.False(t, result)
		assert.Equal(t, 1, src.NumSlots())    // 転送元のスロット数は変わらない
		assert.Equal(t, 50, len(src.Data(0))) // 転送元のデータサイズも変わらない
	})

	t.Run("転送先が空の場合でも正しく転送される", func(t *testing.T) {
		// GIVEN
		srcData := make([]byte, 256)
		src := NewSlottedPage(srcData)
		src.Initialize()
		src.Insert(0, []byte("AAAAA"))
		src.Insert(1, []byte("BBBBB"))

		destData := make([]byte, 256)
		dest := NewSlottedPage(destData)
		dest.Initialize()

		// WHEN
		result := src.TransferAllTo(dest)

		// THEN
		assert.True(t, result)
		assert.Equal(t, 0, src.NumSlots())
		assert.Equal(t, 2, dest.NumSlots())
		assert.Equal(t, []byte("AAAAA"), dest.Data(0))
		assert.Equal(t, []byte("BBBBB"), dest.Data(1))
	})
}

func TestPointerAt(t *testing.T) {
	t.Run("指定したインデックスのポインタが正しく取得できる", func(t *testing.T) {
		// GIVEN
		data := make([]byte, 128)
		sp := NewSlottedPage(data)
		sp.Initialize()
		sp.Insert(0, make([]byte, 10))

		// WHEN
		cellData := sp.pointerAt(0)

		// THEN
		assert.Equal(t, 10, int(cellData.size))
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
