package flst

import (
	"encoding/binary"

	"github.com/ren-yamanashi/minesql/internal/storage/buffer"
)

// base node (16 バイト) 内のオフセット
const (
	baseLengthOffset = 0
	baseFirstOffset  = 4
	baseLastOffset   = 10
)

// node (12 バイト) 内のオフセット
const (
	nodePrevOffset = 0
	nodeNextOffset = 6
)

func readBaseLength(p *buffer.Page, offset int) uint32 {
	body := p.Data().Body()
	return binary.BigEndian.Uint32(body[offset+baseLengthOffset : offset+baseLengthOffset+4])
}

func writeBaseLength(p *buffer.Page, offset int, length uint32) {
	var buf [4]byte
	binary.BigEndian.PutUint32(buf[:], length)
	p.WriteBodyAt(offset+baseLengthOffset, buf[:])
}

func readBaseFirst(p *buffer.Page, offset int) Address {
	return readAddressAt(p, offset+baseFirstOffset)
}

func writeBaseFirst(p *buffer.Page, offset int, addr Address) {
	writeAddressAt(p, offset+baseFirstOffset, addr)
}

func readBaseLast(p *buffer.Page, offset int) Address {
	return readAddressAt(p, offset+baseLastOffset)
}

func writeBaseLast(p *buffer.Page, offset int, addr Address) {
	writeAddressAt(p, offset+baseLastOffset, addr)
}

func readNodePrev(p *buffer.Page, offset int) Address {
	return readAddressAt(p, offset+nodePrevOffset)
}

func writeNodePrev(p *buffer.Page, offset int, addr Address) {
	writeAddressAt(p, offset+nodePrevOffset, addr)
}

func readNodeNext(p *buffer.Page, offset int) Address {
	return readAddressAt(p, offset+nodeNextOffset)
}

func writeNodeNext(p *buffer.Page, offset int, addr Address) {
	writeAddressAt(p, offset+nodeNextOffset, addr)
}

func readAddressAt(p *buffer.Page, offset int) Address {
	return ReadAddress(p.Data().Body(), offset)
}

func writeAddressAt(p *buffer.Page, offset int, addr Address) {
	var buf [addressSize]byte
	addr.WriteAt(buf[:], 0)
	p.WriteBodyAt(offset, buf[:])
}
