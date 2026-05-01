package node

const pointerSize = 4

// Slotted Page のポインタ情報
//   - offset: 2 byte
//   - size: 2 byte
type pointer struct {
	offset uint16
	size   uint16
}

func newPointer(offset, size uint16) pointer {
	return pointer{
		offset: offset,
		size:   size,
	}
}

// Range はポインタが指すデータの開始位置と終了位置を返す (開始位置, 終了位置)
func (p pointer) Range() (start, end int) {
	start = int(p.offset)
	end = start + int(p.size)
	return start, end
}
