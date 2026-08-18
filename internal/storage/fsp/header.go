package fsp

import (
	"encoding/binary"

	"github.com/ren-yamanashi/minesql/internal/storage/buffer"
	"github.com/ren-yamanashi/minesql/internal/storage/flst"
	"github.com/ren-yamanashi/minesql/internal/storage/page"
)

// header は page 0 上の FSP ヘッダーへの読み書きを提供する
type header struct {
	bufPage *buffer.Page
}

// magic はファイルシグネチャを読み取る
func (h header) magic() [4]byte {
	var m [4]byte
	copy(m[:], h.bufPage.Data().Body()[headerMagicOffset:headerMagicOffset+4])
	return m
}

// setMagic はファイルシグネチャに固定値を書き込む
func (h header) setMagic() {
	h.bufPage.WriteBodyAt(headerMagicOffset, []byte(magicValue))
}

// fileId は FileId を読み取る
func (h header) fileId() page.FileId {
	return page.FileId(h.readUint32(headerFileIdOffset))
}

// setFileId は FileId を設定する
func (h header) setFileId(fileId page.FileId) {
	h.writeUint32(headerFileIdOffset, uint32(fileId))
}

// size はファイルの論理ページ数を読み取る
func (h header) size() uint32 {
	return h.readUint32(headerSizeOffset)
}

// setSize はファイルの論理ページ数を設定する
func (h header) setSize(size uint32) {
	h.writeUint32(headerSizeOffset, size)
}

// freeLimit はフリーリミットを読み取る
func (h header) freeLimit() page.PageNumber {
	return page.PageNumber(h.readUint32(headerFreeLimitOffset))
}

// setFreeLimit はフリーリミットを設定する
func (h header) setFreeLimit(limit page.PageNumber) {
	h.writeUint32(headerFreeLimitOffset, uint32(limit))
}

// fragNUsed は FREE_FRAG リスト内の使用中ページ数を読み取る
func (h header) fragNUsed() uint32 {
	return h.readUint32(headerFragNUsedOffset)
}

// setFragNUsed は FREE_FRAG リスト内の使用中ページ数を設定する
func (h header) setFragNUsed(n uint32) {
	h.writeUint32(headerFragNUsedOffset, n)
}

// freeListBase は FREE リストの base node のアドレスを返す
func (h header) freeListBase() flst.Address {
	return flst.Address{PageNumber: 0, Offset: headerFreeListOffset}
}

// freeFragListBase は FREE_FRAG リストの base node のアドレスを返す
func (h header) freeFragListBase() flst.Address {
	return flst.Address{PageNumber: 0, Offset: headerFreeFragListOffset}
}

// fullFragListBase は FULL_FRAG リストの base node のアドレスを返す
func (h header) fullFragListBase() flst.Address {
	return flst.Address{PageNumber: 0, Offset: headerFullFragListOffset}
}

// segId は次に割り当てる segment id を読み取る
func (h header) segId() uint64 {
	return h.readUint64(headerSegIdOffset)
}

// setSegId は次に割り当てる segment id を設定する
func (h header) setSegId(v uint64) {
	h.writeUint64(headerSegIdOffset, v)
}

// segInodesFullBase は SEG_INODES_FULL リストの base node のアドレスを返す
func (h header) segInodesFullBase() flst.Address {
	return flst.Address{PageNumber: 0, Offset: headerSegInodesFullOffset}
}

// segInodesFreeBase は SEG_INODES_FREE リストの base node のアドレスを返す
func (h header) segInodesFreeBase() flst.Address {
	return flst.Address{PageNumber: 0, Offset: headerSegInodesFreeOffset}
}

func (h header) readUint32(offset int) uint32 {
	body := h.bufPage.Data().Body()
	return binary.BigEndian.Uint32(body[offset : offset+4])
}

func (h header) writeUint32(offset int, v uint32) {
	var buf [4]byte
	binary.BigEndian.PutUint32(buf[:], v)
	h.bufPage.WriteBodyAt(offset, buf[:])
}

func (h header) readUint64(offset int) uint64 {
	body := h.bufPage.Data().Body()
	return binary.BigEndian.Uint64(body[offset : offset+8])
}

func (h header) writeUint64(offset int, v uint64) {
	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], v)
	h.bufPage.WriteBodyAt(offset, buf[:])
}
