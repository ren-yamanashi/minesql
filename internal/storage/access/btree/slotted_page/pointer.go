package slottedpage

// Slotted Page の各ポインタのサイズ
//
// offset: 2 byte (0, 1) -- データの開始位置
//
// size: 2 byte (2, 3) -- データのサイズ
const pointerSize = 4

// Slotted Page のポインタ情報
//
// offset: 2 byte (0, 1) -- データの開始位置
//
// size: 2 byte (2, 3) -- データのサイズ
type pointer struct {
	offset uint16 // ポインタが指すデータのオフセット
	size   uint16 // ポインタが指すデータのサイズ
}

// newPointer は指定されたオフセットとサイズから Pointer を生成する
func newPointer(offset, size uint16) pointer {
	return pointer{
		offset: offset,
		size:   size,
	}
}

// Range はポインタが指すデータの開始位置と終了位置を返す (開始位置, 終了位置)
func (p *pointer) Range() (int, int) {
	start := int(p.offset)
	end := start + int(p.size)
	return start, end
}
