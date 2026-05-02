package node

import (
	"encoding/binary"
)

// Slotted Page のヘッダーサイズ
//   - numSlots: 2 byte
//   - freeOffset: 2 byte
//   - pad: 4 byte
const slottedPageHeaderSize = 8

type SlottedPage struct {
	data []byte
}

func NewSlottedPage(data []byte) *SlottedPage {
	return &SlottedPage{data: data}
}

// Insert は指定されたインデックスにサイズ分のデータを挿入する
//   - index: 挿入するスロットのインデックス
//   - data: 挿入するデータ
//   - return: 空き容量が不足している場合は false
func (sp *SlottedPage) Insert(index int, data []byte) bool {
	size := len(data)
	if sp.FreeSpace() < pointerSize+size {
		return false
	}

	// numSlots, freeSpaceOffset の更新
	numSlots := sp.NumSlots()
	freeSpaceOffset := int(binary.BigEndian.Uint16(sp.data[2:4]))
	newFreeSpaceOffset := freeSpaceOffset - size
	binary.BigEndian.PutUint16(sp.data[0:2], uint16(numSlots+1))
	binary.BigEndian.PutUint16(sp.data[2:4], uint16(newFreeSpaceOffset))

	// データを挿入するポインタの index がスロット数より小さい場合は、ポインタ配列をシフト (index 以降を右にずらす)
	if index < numSlots {
		src := slottedPageHeaderSize + index*pointerSize // コピー元の開始位置
		dest := src + pointerSize                        // コピー先の開始位置
		copySize := (numSlots - index) * pointerSize
		copy(sp.data[dest:dest+copySize], sp.data[src:src+copySize])
	}

	// pointer, cell の追加
	sp.setPointer(index, newPointer(
		uint16(newFreeSpaceOffset),
		uint16(size),
	))
	copy(sp.Cell(index), data)
	return true
}

// Delete は指定されたインデックスのデータを削除する
func (sp *SlottedPage) Delete(index int) {
	sp.Resize(index, 0)
	numSlots := sp.NumSlots()

	// ポインタ配列をシフト (index 以降を左にずらす)
	if index < numSlots-1 {
		src := slottedPageHeaderSize + (index+1)*pointerSize // コピー元の開始位置
		dest := slottedPageHeaderSize + index*pointerSize    // コピー先の開始位置
		copySize := (numSlots - index - 1) * pointerSize
		copy(sp.data[dest:dest+copySize], sp.data[src:src+copySize])
	}

	binary.BigEndian.PutUint16(sp.data[0:2], uint16(numSlots-1))
}

// Update は指定されたインデックスのデータを新しいデータに更新する
//   - index: 更新対象のデータが格納されている Slot のインデックス
//   - data: 更新後のデータ
//   - return: 成功した場合は true
func (sp *SlottedPage) Update(index int, data []byte) bool {
	if !sp.Resize(index, len(data)) {
		return false
	}
	copy(sp.Cell(index), data)
	return true
}

// Resize は指定されたインデックスのデータ領域のサイズを変更する
//   - index: サイズを変更するスロットのインデックス
//   - newSize: 新しいサイズ
//   - return: 成功した場合は true, 空き容量が不足している場合は false
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

	freeOffset := int(binary.BigEndian.Uint16(sp.data[2:4]))
	oldFreeOffset := int(pointer.offset)

	// セル配列をシフト (拡張の場合は左に、縮小の場合は右にシフト)
	shiftStart := freeOffset
	shiftEnd := oldFreeOffset
	newFreeOffset := freeOffset - sizeIncrease
	copy(sp.data[newFreeOffset:newFreeOffset+(shiftEnd-shiftStart)], sp.data[shiftStart:shiftEnd])

	// freeSpaceOffset を更新
	binary.BigEndian.PutUint16(sp.data[2:4], uint16(newFreeOffset))

	// 影響を受けるポインタのオフセットを更新
	for i := range sp.NumSlots() {
		p := sp.pointerAt(i)
		if int(p.offset) <= oldFreeOffset {
			p.offset = uint16(int(p.offset) - sizeIncrease)
			sp.setPointer(i, p)
		}
	}

	// ポインタのサイズを更新
	pointer = sp.pointerAt(index) // ループでオフセットが更新されているため再取得
	pointer.size = uint16(newSize)
	if newSize == 0 {
		pointer.offset = uint16(newFreeOffset) // データが削除される場合、オフセットをフリースペースの開始位置に更新
	}
	sp.setPointer(index, pointer)

	return true
}

// TransferAllTo は自身のすべてのスロットを dest の末尾に転送する (自身のスロットはすべて削除される)
//   - dest: 転送先の SlottedPage
//   - return: dest の空き容量が不足している場合は false
func (sp *SlottedPage) TransferAllTo(dest *SlottedPage) bool {
	srcNumSlots := sp.NumSlots()
	if srcNumSlots == 0 {
		return true
	}

	// 必要な空き容量を計算 (ポインタ + データ)
	var totalDataSize int
	for i := range srcNumSlots {
		totalDataSize += len(sp.Cell(i))
	}
	requiredSpace := srcNumSlots*pointerSize + totalDataSize
	if dest.FreeSpace() < requiredSpace {
		return false
	}

	// dest にスロット追加
	destNumSlots := dest.NumSlots()
	destFreeOffset := int(binary.BigEndian.Uint16(dest.data[2:4]))

	for i := range srcNumSlots {
		srcData := sp.Cell(i)
		dataSize := len(srcData)
		destFreeOffset -= dataSize
		copy(dest.data[destFreeOffset:destFreeOffset+dataSize], srcData)
		dest.setPointer(destNumSlots+i, newPointer(uint16(destFreeOffset), uint16(dataSize)))
	}

	binary.BigEndian.PutUint16(dest.data[0:2], uint16(destNumSlots+srcNumSlots))
	binary.BigEndian.PutUint16(dest.data[2:4], uint16(destFreeOffset))
	sp.Initialize()
	return true
}

// Capacity は ヘッダー領域を除いた Slotted Page の容量を返す
func (sp *SlottedPage) Capacity() int {
	return len(sp.data) - slottedPageHeaderSize
}

// NumSlots は Slotted Page のヘッダーから現在のスロット数を読み取る
func (sp *SlottedPage) NumSlots() int {
	return int(binary.BigEndian.Uint16(sp.data[0:2]))
}

// FreeSpace は Slotted Page の空き領域のサイズを取得する
func (sp *SlottedPage) FreeSpace() int {
	offset := int(binary.BigEndian.Uint16(sp.data[2:4]))
	pointersSize := pointerSize * sp.NumSlots()
	return offset - pointersSize - slottedPageHeaderSize
}

// Cell は指定されたインデックスのセル (データ) を取得する
func (sp *SlottedPage) Cell(index int) []byte {
	pointer := sp.pointerAt(index)
	start, end := pointer.Range()
	return sp.data[start:end]
}

// Initialize は Slotted Page を初期化する
func (sp *SlottedPage) Initialize() {
	binary.BigEndian.PutUint16(sp.data[0:2], 0)
	binary.BigEndian.PutUint16(sp.data[2:4], uint16(len(sp.data)))
	binary.BigEndian.PutUint32(sp.data[4:8], 0)
}

// pointerAt は指定されたインデックスのポインタを取得する
func (sp *SlottedPage) pointerAt(index int) pointer {
	base := slottedPageHeaderSize + index*pointerSize
	return newPointer(
		binary.BigEndian.Uint16(sp.data[base:base+2]),
		binary.BigEndian.Uint16(sp.data[base+2:base+4]),
	)
}

// setPointer は指定されたインデックスのポインタを設定する
func (sp *SlottedPage) setPointer(index int, p pointer) {
	base := slottedPageHeaderSize + index*pointerSize
	binary.BigEndian.PutUint16(sp.data[base:base+2], p.offset) // offset
	binary.BigEndian.PutUint16(sp.data[base+2:base+4], p.size) // size
}
