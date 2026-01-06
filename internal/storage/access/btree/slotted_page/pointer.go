package slottedpage

const pointerSize = 4

// Slotted Page のポインタ情報
// offset: 2 byte (0, 1)
// size: 2 byte (2, 3)
type Pointer struct {
	// ポインタが指すデータのオフセット
	offset uint16
	// ポインタが指すデータのサイズ
	size uint16
}

func NewPointer(offset, size uint16) Pointer {
	return Pointer{
		offset: offset,
		size:   size,
	}
}

// ポインタが指すデータの開始位置と終了位置を返す (開始位置, 終了位置)
func (p *Pointer) Range() (int, int) {
	start := int(p.offset)
	end := start + int(p.size)
	return start, end
}
