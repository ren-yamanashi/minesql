package slottedpage

import "encoding/binary"

type SlottedPage struct {
	data []byte
}

// NewSlottedPage は指定されたバイトスライスを基に Slotted Page を生成する
func NewSlottedPage(data []byte) *SlottedPage {
	return &SlottedPage{data: data}
}

// Capacity は ヘッダー領域を除いた Slotted Page の容量を返す
func (sp *SlottedPage) Capacity() int {
	return len(sp.data) - headerSize
}

// NumSlots は Slotted Page のヘッダーから現在のスロット数を読み取る
func (sp *SlottedPage) NumSlots() int {
	return int(binary.BigEndian.Uint16(sp.data[0:2]))
}

// FreeSpace は Slotted Page の空き領域のサイズを返す
// see: docs/architecture/access/b+tree/slotted-page.md#フリースペースのサイズの算出例
func (sp *SlottedPage) FreeSpace() int {
	freeSpaceOffset := int(binary.BigEndian.Uint16(sp.data[2:4])) // フリースペースの開始位置 (offset) はヘッダーの 2 バイト目から 2 バイト分に格納されている
	pointersSize := pointerSize * sp.NumSlots()
	return freeSpaceOffset - pointersSize - headerSize
}

// Data は指定されたインデックスのデータを取得する
func (sp *SlottedPage) Data(index int) []byte {
	pointer := sp.pointerAt(index)
	start, end := pointer.Range()
	return sp.data[start:end]
}

// Initialize は Slotted Page を初期化する
func (sp *SlottedPage) Initialize() {
	binary.BigEndian.PutUint16(sp.data[0:2], 0)                    // numSlots
	binary.BigEndian.PutUint16(sp.data[2:4], uint16(len(sp.data))) // freeOffset = end of data
	binary.BigEndian.PutUint32(sp.data[4:8], 0)                    // pad
}

// Insert は指定されたインデックスにサイズ分のデータを挿入する (領域の確保のみを行い、実際のデータの書き込みは行わない)
// index: 挿入するスロットのインデックス
// data: 挿入するデータ
// 空き容量が不足している場合は false を返す
func (sp *SlottedPage) Insert(index int, data []byte) bool {
	size := len(data)

	if sp.FreeSpace() < pointerSize+size {
		return false
	}

	numSlots := sp.NumSlots()
	freeSpaceOffset := int(binary.BigEndian.Uint16(sp.data[2:4]))

	// freeSpaceOffset を減らす
	newFreeSpaceOffset := freeSpaceOffset - size                         // 追加するデータのサイズ分だけ freeSpaceOffset を減らす
	binary.BigEndian.PutUint16(sp.data[2:4], uint16(newFreeSpaceOffset)) // ヘッダーのフリースペースの開始位置を更新

	// numSlots を増やす
	binary.BigEndian.PutUint16(sp.data[0:2], uint16(numSlots+1)) // ヘッダーのスロット数を更新

	// データを挿入するポインタの index がスロット数より小さい場合は、ポインタ配列をシフト (index 以降を右にずらす)
	if index < numSlots {
		src := headerSize + index*pointerSize // コピー元の開始位置
		destination := src + pointerSize      // コピー先の開始位置
		copySize := (numSlots - index) * pointerSize
		copy(sp.data[destination:destination+copySize], sp.data[src:src+copySize])
	}

	// 新しいポインタを設定
	sp.setPointer(index, newPointer(
		uint16(newFreeSpaceOffset),
		uint16(size),
	))

	// セル配列にデータを追加
	copy(sp.Data(index), data) // sp.Data(index) は新しいポインタが指す位置を返すため、そこにデータを書き込む (Insert 時は常にセル配列の末尾が新しいデータの位置になる)
	return true
}

// Resize は指定されたインデックスのデータ領域のサイズを変更する
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

	freeSpaceOffset := int(binary.BigEndian.Uint16(sp.data[2:4]))
	oldOffset := int(pointer.offset)

	// データ領域 (セル配列) をシフトする
	// 対象のデータの後ろにあるデータを、拡張・縮小に応じて左または右にシフトする (拡張の場合は左に、縮小の場合は右にシフトする)
	shiftStart := freeSpaceOffset
	shiftEnd := oldOffset
	newFreeSpaceOffset := freeSpaceOffset - sizeIncrease
	copy(sp.data[newFreeSpaceOffset:newFreeSpaceOffset+(shiftEnd-shiftStart)], sp.data[shiftStart:shiftEnd])

	// freeSpaceOffset を更新
	binary.BigEndian.PutUint16(sp.data[2:4], uint16(newFreeSpaceOffset))

	// 影響を受けるポインタのオフセットを更新
	for i := 0; i < sp.NumSlots(); i++ {
		p := sp.pointerAt(i)
		// ポインタのオフセットが oldOffset 以下の場合 (つまりデータが oldOffset より左に位置する場合) は、サイズ変更によってデータ領域がシフトされるため、オフセットを更新する
		if int(p.offset) <= oldOffset {
			p.offset = uint16(int(p.offset) - sizeIncrease) // サイズ変更によってシフトされた分だけオフセットを更新
			sp.setPointer(i, p)
		}
	}

	// 対象のポインタのサイズを更新
	pointer = sp.pointerAt(index) // ループでオフセットが更新されているため再取得する
	pointer.size = uint16(newSize)
	// データが縮小されてサイズが 0 になる場合 (=データが削除される場合) は、データが存在しないためオフセットをフリースペースの開始位置に更新する
	if newSize == 0 {
		pointer.offset = uint16(newFreeSpaceOffset)
	}
	sp.setPointer(index, pointer)

	return true
}

// Remove は指定されたインデックスのデータを削除する
func (sp *SlottedPage) Remove(index int) {
	// データサイズを 0 にする
	sp.Resize(index, 0)

	numSlots := sp.NumSlots()

	// ポインタ配列をシフト (index 以降を左にずらす)
	if index < numSlots-1 {
		src := headerSize + (index+1)*pointerSize     // コピー元の開始位置
		destination := headerSize + index*pointerSize // コピー先の開始位置
		copySize := (numSlots - index - 1) * pointerSize
		copy(sp.data[destination:destination+copySize], sp.data[src:src+copySize])
	}

	// numSlots を減らす
	binary.BigEndian.PutUint16(sp.data[0:2], uint16(numSlots-1))
}

// TransferAllTo は自分のすべてのスロットを dest の末尾に転送する。(自身のスロットはすべて削除される)
// 空き容量が不足している場合は false を返す
func (sp *SlottedPage) TransferAllTo(dest *SlottedPage) bool {
	srcNumSlots := sp.NumSlots()
	if srcNumSlots == 0 {
		return true
	}

	// 必要な空き容量を計算 (ポインタ + データ)
	totalDataSize := 0
	for i := range srcNumSlots {
		totalDataSize += len(sp.Data(i))
	}
	requiredSpace := srcNumSlots*pointerSize + totalDataSize

	if dest.FreeSpace() < requiredSpace {
		return false
	}

	// dest にスロットを追加
	destNumSlots := dest.NumSlots()
	destFreeOffset := int(binary.BigEndian.Uint16(dest.data[2:4]))

	for i := range srcNumSlots {
		srcData := sp.Data(i)
		dataSize := len(srcData)

		// データをコピー
		destFreeOffset -= dataSize
		copy(dest.data[destFreeOffset:destFreeOffset+dataSize], srcData)

		// ポインタを設定
		dest.setPointer(destNumSlots+i, newPointer(uint16(destFreeOffset), uint16(dataSize)))
	}

	// dest のヘッダーを更新
	binary.BigEndian.PutUint16(dest.data[0:2], uint16(destNumSlots+srcNumSlots))
	binary.BigEndian.PutUint16(dest.data[2:4], uint16(destFreeOffset))

	// 自身のスロットをクリア
	sp.Initialize()

	return true
}

// 指定されたインデックスのポインタを取得する
func (sp *SlottedPage) pointerAt(index int) pointer {
	base := headerSize + index*pointerSize
	return newPointer(
		binary.BigEndian.Uint16(sp.data[base:base+2]),
		binary.BigEndian.Uint16(sp.data[base+2:base+4]),
	)
}

// 指定されたインデックスのポインタを設定する
func (sp *SlottedPage) setPointer(index int, pointer pointer) {
	base := headerSize + index*pointerSize
	binary.BigEndian.PutUint16(sp.data[base:base+2], pointer.offset) // offset
	binary.BigEndian.PutUint16(sp.data[base+2:base+4], pointer.size) // size
}
