package fsp

import (
	"encoding/binary"
	"fmt"

	"github.com/ren-yamanashi/minesql/internal/storage/buffer"
	"github.com/ren-yamanashi/minesql/internal/storage/flst"
	"github.com/ren-yamanashi/minesql/internal/storage/page"
)

// inodeEntry は inode ページ上の 1 つの inode エントリへの読み書きを提供する
type inodeEntry struct {
	bufPage *buffer.Page
	index   int
}

// offset はエントリ先頭の body 相対オフセットを返す
func (e inodeEntry) offset() int {
	return inodeArrOffset + e.index*inodeEntrySize
}

// segId は segment id を読み取る (0 = 未使用スロット)
func (e inodeEntry) segId() uint64 {
	body := e.bufPage.Data().Body()
	off := e.offset() + inodeSegIdOffset
	return binary.BigEndian.Uint64(body[off : off+8])
}

// setSegId は segment id を設定する
func (e inodeEntry) setSegId(v uint64) {
	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], v)
	e.bufPage.WriteBodyAt(e.offset()+inodeSegIdOffset, buf[:])
}

// notFullNUsed は NOT_FULL リスト内の使用中ページ数を読み取る
func (e inodeEntry) notFullNUsed() uint32 {
	body := e.bufPage.Data().Body()
	off := e.offset() + inodeNotFullNUsedOffset
	return binary.BigEndian.Uint32(body[off : off+4])
}

// setNotFullNUsed は NOT_FULL リスト内の使用中ページ数を設定する
func (e inodeEntry) setNotFullNUsed(v uint32) {
	var buf [4]byte
	binary.BigEndian.PutUint32(buf[:], v)
	e.bufPage.WriteBodyAt(e.offset()+inodeNotFullNUsedOffset, buf[:])
}

// freeListBase は自エントリ内の FREE リストの base node のアドレスを返す
func (e inodeEntry) freeListBase() flst.Address {
	return flst.Address{
		PageNumber: e.bufPage.PageId().PageNumber(),
		Offset:     uint16(e.offset() + inodeFreeListOffset),
	}
}

// notFullListBase は自エントリ内の NOT_FULL リストの base node のアドレスを返す
func (e inodeEntry) notFullListBase() flst.Address {
	return flst.Address{
		PageNumber: e.bufPage.PageId().PageNumber(),
		Offset:     uint16(e.offset() + inodeNotFullListOffset),
	}
}

// fullListBase は自エントリ内の FULL リストの base node のアドレスを返す
func (e inodeEntry) fullListBase() flst.Address {
	return flst.Address{
		PageNumber: e.bufPage.PageId().PageNumber(),
		Offset:     uint16(e.offset() + inodeFullListOffset),
	}
}

// magic は破損検出用マジックナンバーを読み取る
func (e inodeEntry) magic() [4]byte {
	var m [4]byte
	copy(m[:], e.bufPage.Data().Body()[e.offset()+inodeMagicOffset:e.offset()+inodeMagicOffset+4])
	return m
}

// setMagic は破損検出用マジックナンバーに固定値を書き込む
func (e inodeEntry) setMagic() {
	e.bufPage.WriteBodyAt(e.offset()+inodeMagicOffset, []byte(inodeMagicValue))
}

// clearMagic は破損検出用マジックナンバーをゼロ埋めする
func (e inodeEntry) clearMagic() {
	e.bufPage.WriteBodyAt(e.offset()+inodeMagicOffset, make([]byte, 4))
}

// fragSlot は frag array 内の slot i の PageNumber を読み取る
//   - i が範囲外の場合は panic する (以下の frag slot 操作も同様)
func (e inodeEntry) fragSlot(i int) page.PageNumber {
	e.assertSlotIndex(i)
	body := e.bufPage.Data().Body()
	off := e.offset() + inodeFragArrOffset + i*4
	return page.PageNumber(binary.BigEndian.Uint32(body[off : off+4]))
}

// setFragSlot は frag array 内の slot i の PageNumber を設定する
func (e inodeEntry) setFragSlot(i int, pn page.PageNumber) {
	e.assertSlotIndex(i)
	var buf [4]byte
	binary.BigEndian.PutUint32(buf[:], uint32(pn))
	e.bufPage.WriteBodyAt(e.offset()+inodeFragArrOffset+i*4, buf[:])
}

// firstFreeFragSlot は未使用 slot の最小 index を返す (満杯なら -1)
func (e inodeEntry) firstFreeFragSlot() int {
	for i := range inodeFragSlotCount {
		if e.fragSlot(i) == page.MaxPageNumber {
			return i
		}
	}
	return -1
}

// lastUsedFragSlot は使用中 slot の最大 index を返す (すべて未使用なら -1)
func (e inodeEntry) lastUsedFragSlot() int {
	for i := inodeFragSlotCount - 1; i >= 0; i-- {
		if e.fragSlot(i) != page.MaxPageNumber {
			return i
		}
	}
	return -1
}

// fragPageCount は frag array の使用中 slot 数を返す
func (e inodeEntry) fragPageCount() int {
	n := 0
	for i := range inodeFragSlotCount {
		if e.fragSlot(i) != page.MaxPageNumber {
			n++
		}
	}
	return n
}

// address は自エントリのアドレスを返す (= segment header に書かれる値)
func (e inodeEntry) address() flst.Address {
	return flst.Address{
		PageNumber: e.bufPage.PageId().PageNumber(),
		Offset:     uint16(e.offset()),
	}
}

// initializeEntry は使用開始時のエントリ内容を書き込む
//   - segId / notFullNUsed 0 / magic / frag array の全 slot を無効値で埋める
//   - 3 リストの base の初期化は呼び出し側の責務 (flst.InitBase を使う)
func (e inodeEntry) initializeEntry(segId uint64) {
	e.setSegId(segId)
	e.setNotFullNUsed(0)
	e.setMagic()
	for i := range inodeFragSlotCount {
		e.setFragSlot(i, page.MaxPageNumber)
	}
}

func (e inodeEntry) assertSlotIndex(i int) {
	if i < 0 || i >= inodeFragSlotCount {
		panic(fmt.Sprintf("fsp: inode frag slot index out of range: %d", i))
	}
}
