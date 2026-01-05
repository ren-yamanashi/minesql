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

// Slotted Page の空き領域のサイズを返す (以下の図を参照)
// pageSize = 4096 bytes
// headerSize = 8 bytes
// numSlots = 3
// pointerSize = 4 bytes
// |ヘッダー|Ptr0|Ptr1|Ptr2|========空き領域========|Cell2|Cell1|Cell0|
// 0       8    12   16  20                      4000              4096
// 　　　　　↑___12 bytes___↑                      ↑
// 　　　　　pointersSize                          freeSpaceOffset
func (sp *SlottedPage) FreeSpace() int {
	freeSpaceOffset := int(binary.LittleEndian.Uint16(sp.data[2:4]))
	pointersSize := pointerSize * sp.NumSlots()
	return freeSpaceOffset - pointersSize - headerSize
}

// 指定されたインデックスのポインタを取得する
func (sp *SlottedPage) pointerAt(index int) Pointer {
	base := headerSize + index*pointerSize
	return Pointer{
		offset: binary.LittleEndian.Uint16(sp.data[base : base+2]),
		size:   binary.LittleEndian.Uint16(sp.data[base+2 : base+4]),
	}
}

// 指定されたインデックスのデータを取得する
func (sp *SlottedPage) Data(index int) []byte {
	pointer := sp.pointerAt(index)
	start, end := pointer.Range()
	return sp.data[start:end]
}
