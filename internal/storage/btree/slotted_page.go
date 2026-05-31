package btree

import (
	"encoding/binary"
)

// Slotted Page のヘッダーサイズ
//   - numSlots: 2 byte
//   - freeOffset: 2 byte
//   - pad: 4 byte
const slottedPageHeaderSize = 8

type slottedPage struct {
	data []byte
}

func newSlottedPage(data []byte) *slottedPage {
	return &slottedPage{data: data}
}

// hasSpaceFor は指定サイズの追加データが空き領域に収まるかを返す
func (sp *slottedPage) hasSpaceFor(dataSize int) bool {
	return sp.freeSpace() >= slottedPagePointerSize+dataSize
}

// insert は指定されたインデックスにサイズ分のデータを挿入する
//   - index: 挿入するスロットのインデックス
//   - data: 挿入するデータ
//   - return: 空き容量が不足している場合は false
func (sp *slottedPage) insert(index int, data []byte) bool {
	size := len(data)
	if sp.freeSpace() < slottedPagePointerSize+size {
		return false
	}

	// numSlots, freeSpaceOffset の更新
	numSlots := sp.numSlots()
	freeSpaceOffset := int(binary.BigEndian.Uint16(sp.data[2:4]))
	newFreeSpaceOffset := freeSpaceOffset - size
	binary.BigEndian.PutUint16(sp.data[0:2], uint16(numSlots+1))
	binary.BigEndian.PutUint16(sp.data[2:4], uint16(newFreeSpaceOffset))

	// データを挿入するポインタの index がスロット数より小さい場合は、ポインタ配列をシフト (index 以降を右にずらす)
	if index < numSlots {
		src := slottedPageHeaderSize + index*slottedPagePointerSize // コピー元の開始位置
		dest := src + slottedPagePointerSize                        // コピー先の開始位置
		copySize := (numSlots - index) * slottedPagePointerSize
		copy(sp.data[dest:dest+copySize], sp.data[src:src+copySize])
	}

	// pointer, cell の追加
	sp.setPointer(index, newPointer(
		uint16(newFreeSpaceOffset),
		uint16(size),
	))
	copy(sp.cell(index), data)
	return true
}

// delete は指定されたインデックスのデータを削除する
func (sp *slottedPage) delete(index int) {
	sp.resize(index, 0)
	numSlots := sp.numSlots()

	// ポインタ配列をシフト (index 以降を左にずらす)
	if index < numSlots-1 {
		src := slottedPageHeaderSize + (index+1)*slottedPagePointerSize // コピー元の開始位置
		dest := slottedPageHeaderSize + index*slottedPagePointerSize    // コピー先の開始位置
		copySize := (numSlots - index - 1) * slottedPagePointerSize
		copy(sp.data[dest:dest+copySize], sp.data[src:src+copySize])
	}

	binary.BigEndian.PutUint16(sp.data[0:2], uint16(numSlots-1))
}

// update は指定されたインデックスのデータを新しいデータに更新する
//   - index: 更新対象のデータが格納されている Slot のインデックス
//   - data: 更新後のデータ
//   - return: 成功した場合は true
func (sp *slottedPage) update(index int, data []byte) bool {
	if !sp.resize(index, len(data)) {
		return false
	}
	copy(sp.cell(index), data)
	return true
}

// resize は指定されたインデックスのデータ領域のサイズを変更する
//   - index: サイズを変更するスロットのインデックス
//   - newSize: 新しいサイズ
//   - return: 成功した場合は true, 空き容量が不足している場合は false
func (sp *slottedPage) resize(index int, newSize int) bool {
	pointer := sp.pointerAt(index)
	oldSize := int(pointer.size)
	sizeIncrease := newSize - oldSize

	if sizeIncrease == 0 {
		return true
	}
	if sizeIncrease > sp.freeSpace() {
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
	for i := range sp.numSlots() {
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

// transferAllTo は自身のすべてのスロットを dest の末尾に転送する (自身のスロットはすべて削除される)
//   - dest: 転送先の SlottedPage
//   - return: dest の空き容量が不足している場合は false
func (sp *slottedPage) transferAllTo(dest *slottedPage) bool {
	srcNumSlots := sp.numSlots()
	if srcNumSlots == 0 {
		return true
	}

	// 必要な空き容量を計算 (ポインタ + データ)
	var totalDataSize int
	for i := range srcNumSlots {
		totalDataSize += len(sp.cell(i))
	}
	requiredSpace := srcNumSlots*slottedPagePointerSize + totalDataSize
	if dest.freeSpace() < requiredSpace {
		return false
	}

	// dest にスロット追加
	destNumSlots := dest.numSlots()
	destFreeOffset := int(binary.BigEndian.Uint16(dest.data[2:4]))

	for i := range srcNumSlots {
		srcData := sp.cell(i)
		dataSize := len(srcData)
		destFreeOffset -= dataSize
		copy(dest.data[destFreeOffset:destFreeOffset+dataSize], srcData)
		dest.setPointer(destNumSlots+i, newPointer(uint16(destFreeOffset), uint16(dataSize)))
	}

	binary.BigEndian.PutUint16(dest.data[0:2], uint16(destNumSlots+srcNumSlots))
	binary.BigEndian.PutUint16(dest.data[2:4], uint16(destFreeOffset))
	sp.initialize()
	return true
}

// capacity は ヘッダー領域を除いた Slotted Page の容量を返す
func (sp *slottedPage) capacity() int {
	return len(sp.data) - slottedPageHeaderSize
}

// numSlots は Slotted Page のヘッダーから現在のスロット数を読み取る
func (sp *slottedPage) numSlots() int {
	return int(binary.BigEndian.Uint16(sp.data[0:2]))
}

// freeSpace は Slotted Page の空き領域のサイズを取得する
func (sp *slottedPage) freeSpace() int {
	offset := int(binary.BigEndian.Uint16(sp.data[2:4]))
	pointersSize := slottedPagePointerSize * sp.numSlots()
	return offset - pointersSize - slottedPageHeaderSize
}

// cell は指定されたインデックスのセル (データ) を取得する
func (sp *slottedPage) cell(index int) []byte {
	pointer := sp.pointerAt(index)
	start, end := pointer.bounds()
	return sp.data[start:end]
}

// initialize は Slotted Page を初期化する
func (sp *slottedPage) initialize() {
	binary.BigEndian.PutUint16(sp.data[0:2], 0)
	binary.BigEndian.PutUint16(sp.data[2:4], uint16(len(sp.data)))
	binary.BigEndian.PutUint32(sp.data[4:8], 0)
}

// pointerAt は指定されたインデックスのポインタを取得する
func (sp *slottedPage) pointerAt(index int) pointer {
	base := slottedPageHeaderSize + index*slottedPagePointerSize
	return newPointer(
		binary.BigEndian.Uint16(sp.data[base:base+2]),
		binary.BigEndian.Uint16(sp.data[base+2:base+4]),
	)
}

// setPointer は指定されたインデックスのポインタを設定する
func (sp *slottedPage) setPointer(index int, p pointer) {
	base := slottedPageHeaderSize + index*slottedPagePointerSize
	binary.BigEndian.PutUint16(sp.data[base:base+2], p.offset) // offset
	binary.BigEndian.PutUint16(sp.data[base+2:base+4], p.size) // size
}
