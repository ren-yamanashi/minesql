package slottedpage

import "encoding/binary"

type SlottedPage struct {
	data []byte
}

func NewSlottedPage(data []byte) *SlottedPage {
	return &SlottedPage{data: data}
}

// Slotted Page の容量 (ヘッダー領域を除く) を返す
func (sp *SlottedPage) Capacity() int {
	return len(sp.data) - headerSize
}

// Slotted Page のヘッダーから現在のスロット数を読み取る
func (sp *SlottedPage) NumSlots() int {
	return int(binary.LittleEndian.Uint16(sp.data[0:2]))
}

// Slotted Page の空き領域のサイズを返す
func (sp *SlottedPage) FreeSpace() int {
	freeSpaceOffset := int(binary.LittleEndian.Uint16(sp.data[2:4]))
	pointersSize := pointerSize * sp.NumSlots()
	// MEMO: `- headerSize` しない方が良いかも
	return freeSpaceOffset - pointersSize - headerSize
}

// 指定されたインデックスのポインタを取得する
func (sp *SlottedPage) pointerAt(index int) Pointer {
	base := headerSize + index*pointerSize
	return NewPointer(
		binary.LittleEndian.Uint16(sp.data[base:base+2]),
		binary.LittleEndian.Uint16(sp.data[base+2:base+4]),
	)
}

// 指定されたインデックスのデータを取得する
func (sp *SlottedPage) Data(index int) []byte {
	pointer := sp.pointerAt(index)
	start, end := pointer.Range()
	return sp.data[start:end]
}

// Slotted Page を初期化する
func (sp *SlottedPage) Initialize() {
	binary.LittleEndian.PutUint16(sp.data[0:2], 0)                    // numSlots
	binary.LittleEndian.PutUint16(sp.data[2:4], uint16(len(sp.data))) // freeOffset = end of data
	binary.LittleEndian.PutUint32(sp.data[4:8], 0)                    // _pad
}

// 指定されたインデックスのポインタを設定する
func (sp *SlottedPage) setPointer(index int, pointer Pointer) {
	base := headerSize + index*pointerSize
	binary.LittleEndian.PutUint16(sp.data[base:base+2], pointer.offset) // offset
	binary.LittleEndian.PutUint16(sp.data[base+2:base+4], pointer.size) // size
}

// 指定されたインデックスにサイズ分のデータを挿入する (領域の確保のみを行い、実際のデータの書き込みは行わない)
// size: 挿入するデータのサイズ
// 空き容量が不足している場合は false を返す
// このメソッドを利用する場合、実行後に Data メソッドで取得したバイトスライスに対してデータを書き込む必要がある (以下実装例)
// ```go
//
//	if slottedPage.Insert(bufferId, dataSize) {
//	    copy(slottedPage.Data(bufferId), dataBytes)
//	}
//
// ```
func (sp *SlottedPage) Insert(index int, size int) bool {
	if sp.FreeSpace() < pointerSize+size {
		return false
	}

	numSlots := sp.NumSlots()
	freeSpaceOffset := int(binary.LittleEndian.Uint16(sp.data[2:4]))

	// freeSpaceOffset を減らす
	newFreeSpaceOffset := freeSpaceOffset - size
	binary.LittleEndian.PutUint16(sp.data[2:4], uint16(newFreeSpaceOffset))

	// numSlots を増やす
	binary.LittleEndian.PutUint16(sp.data[0:2], uint16(numSlots+1))

	// ポインタ配列をシフト (index 以降を右にずらす)
	if index < numSlots {
		src := headerSize + index*pointerSize // コピー元の開始位置
		dst := src + pointerSize              // コピー先の開始位置
		copySize := (numSlots - index) * pointerSize
		copy(sp.data[dst:dst+copySize], sp.data[src:src+copySize])
	}

	// 新しいポインタを設定
	sp.setPointer(index, NewPointer(
		uint16(newFreeSpaceOffset),
		uint16(size),
	))

	return true
}

// 指定されたインデックスのデータ領域のサイズを変更する
// 空き容量が不足している場合は false を返す
func (sp *SlottedPage) Resize(index int, newSize int) bool {
	pointer := sp.pointerAt(index)
	oldSize := int(pointer.size)
	sizeIncrease := newSize - oldSize

	if sizeIncrease == 0 {
		return true
	}

	if sizeIncrease > sp.FreeSpace() {
		return false
	}

	freeSpaceOffset := int(binary.LittleEndian.Uint16(sp.data[2:4]))
	oldOffset := int(pointer.offset)

	// データ領域をシフト (空き領域を拡張または縮小)
	shiftStart := freeSpaceOffset
	shiftEnd := oldOffset
	newFreeSpaceOffset := freeSpaceOffset - sizeIncrease
	copy(sp.data[newFreeSpaceOffset:newFreeSpaceOffset+(shiftEnd-shiftStart)], sp.data[shiftStart:shiftEnd])

	// freeSpaceOffset を更新
	binary.LittleEndian.PutUint16(sp.data[2:4], uint16(newFreeSpaceOffset))

	// 影響を受けるポインタのオフセットを更新
	for i := 0; i < sp.NumSlots(); i++ {
		p := sp.pointerAt(i)
		if int(p.offset) <= oldOffset {
			p.offset = uint16(int(p.offset) - sizeIncrease)
			sp.setPointer(i, p)
		}
	}

	// 対象のポインタのサイズを更新
	pointer.size = uint16(newSize)
	if newSize == 0 {
		pointer.offset = uint16(newFreeSpaceOffset)
	}
	sp.setPointer(index, pointer)

	return true
}

// 指定されたインデックスのデータを削除する
func (sp *SlottedPage) Remove(index int) {
	sp.Resize(index, 0)

	numSlots := sp.NumSlots()

	// ポインタ配列をシフト (index 以降を左にずらす)
	if index < numSlots-1 {
		src := headerSize + (index+1)*pointerSize // コピー元の開始位置
		dst := headerSize + index*pointerSize     // コピー先の開始位置
		copySize := (numSlots - index - 1) * pointerSize
		copy(sp.data[dst:dst+copySize], sp.data[src:src+copySize])
	}

	// numSlots を減らす
	binary.LittleEndian.PutUint16(sp.data[0:2], uint16(numSlots-1))
}
