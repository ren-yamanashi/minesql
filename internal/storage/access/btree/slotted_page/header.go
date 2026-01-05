package slottedpage

const headerSize = 8

// Slotted Page のヘッダー情報
// numSlots: 2 byte (0, 1)
// freeOffset: 2 byte (2, 3)
// _pad: 4 byte (4, 5, 6, 7)
type Header struct {
	numSlots   uint16
	freeOffset uint16
	_pad       uint32
}
