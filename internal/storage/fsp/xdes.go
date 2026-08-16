package fsp

import (
	"encoding/binary"
	"fmt"
	"math/bits"

	"github.com/ren-yamanashi/minesql/internal/storage/buffer"
	"github.com/ren-yamanashi/minesql/internal/storage/flst"
)

// xdesEntry は記述子ページ上の 1 つの extent 記述子への読み書きを提供する
type xdesEntry struct {
	bufPage *buffer.Page
	index   int
}

// offset はエントリ先頭の body 相対オフセットを返す
func (x xdesEntry) offset() int {
	return headerSize + x.index*xdesEntrySize
}

// state は extent の状態を読み取る
func (x xdesEntry) state() extentState {
	body := x.bufPage.Data().Body()
	off := x.offset() + xdesStateOffset
	return extentState(binary.BigEndian.Uint32(body[off : off+4]))
}

// setState は extent の状態を設定する
//   - 許容されない遷移の場合は panic する
func (x xdesEntry) setState(to extentState) {
	assertStateTransition(x.state(), to)
	var buf [4]byte
	binary.BigEndian.PutUint32(buf[:], uint32(to))
	x.bufPage.WriteBodyAt(x.offset()+xdesStateOffset, buf[:])
}

// isPageFree は extent 内ページ位置 pos の free bit を返す (true = free)
//   - pos が extent 内ページ数の範囲外の場合は panic する (以下の free bit 操作も同様)
func (x xdesEntry) isPageFree(pos int) bool {
	x.assertPagePos(pos)
	b := x.bufPage.Data().Body()[x.offset()+xdesBitmapOffset+pos/8]
	return (b>>(pos%8))&1 == 1
}

// setPageFree は extent 内ページ位置 pos の free bit を設定する (true = free)
func (x xdesEntry) setPageFree(pos int, free bool) {
	x.assertPagePos(pos)
	byteOffset := x.offset() + xdesBitmapOffset + pos/8
	b := x.bufPage.Data().Body()[byteOffset]
	if free {
		b |= 1 << (pos % 8)
	} else {
		b &^= 1 << (pos % 8)
	}
	x.bufPage.WriteBodyAt(byteOffset, []byte{b})
}

// isAllFree は extent 内の全ページが free かどうかを返す
func (x xdesEntry) isAllFree() bool {
	for _, b := range x.bitmap() {
		if b != 0xFF {
			return false
		}
	}
	return true
}

// isAllUsed は extent 内の全ページが used かどうかを返す
func (x xdesEntry) isAllUsed() bool {
	for _, b := range x.bitmap() {
		if b != 0x00 {
			return false
		}
	}
	return true
}

// firstFreePos は extent 内で最も小さいページ位置の free bit の位置を返す
//   - 空きがない場合は -1 を返す
func (x xdesEntry) firstFreePos() int {
	for i, b := range x.bitmap() {
		if b == 0 {
			continue
		}
		return i*8 + bits.TrailingZeros8(b)
	}
	return -1
}

// freePageCount は extent 内の free bit の総数を返す
func (x xdesEntry) freePageCount() int {
	n := 0
	for _, b := range x.bitmap() {
		n += bits.OnesCount8(b)
	}
	return n
}

// flstNodeAddress は自身の flst node のアドレスを返す
func (x xdesEntry) flstNodeAddress() flst.Address {
	return flst.Address{
		PageNumber: x.bufPage.PageId().PageNumber(),
		Offset:     uint16(x.offset() + xdesFlstNodeOffset),
	}
}

// initialize はエントリを初期化する (予約領域 0 / node の前後は無効アドレス / 状態 FREE / 全ページ free)
//   - 状態が NOT_INITED のエントリに対してのみ呼び出せる
func (x xdesEntry) initialize() {
	x.bufPage.WriteBodyAt(x.offset()+xdesIdOffset, make([]byte, xdesFlstNodeOffset-xdesIdOffset))
	var node [xdesStateOffset - xdesFlstNodeOffset]byte
	flst.InvalidAddress().WriteAt(node[:], 0) // prev
	flst.InvalidAddress().WriteAt(node[:], 6) // next
	x.bufPage.WriteBodyAt(x.offset()+xdesFlstNodeOffset, node[:])
	x.setState(stateFree)
	bitmap := make([]byte, xdesBitmapSize)
	for i := range bitmap {
		bitmap[i] = 0xFF
	}
	x.bufPage.WriteBodyAt(x.offset()+xdesBitmapOffset, bitmap)
}

func (x xdesEntry) bitmap() []byte {
	off := x.offset() + xdesBitmapOffset
	return x.bufPage.Data().Body()[off : off+xdesBitmapSize]
}

func (x xdesEntry) assertPagePos(pos int) {
	if pos < 0 || pos >= extentPageCount {
		panic(fmt.Sprintf("fsp: extent page position out of range: %d", pos))
	}
}
