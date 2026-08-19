package fsp

import (
	"github.com/ren-yamanashi/minesql/internal/storage/page"
)

// magicValue は全ファイルの page 0 の body 先頭に置かれるファイルシグネチャ
const magicValue = "MINE"

// FSP ヘッダー (計 108 バイト) 内のオフセット (page 0 の body 先頭からの相対値)
const (
	headerMagicOffset         = 0
	headerFileIdOffset        = 4
	headerSizeOffset          = 8
	headerFreeLimitOffset     = 12
	headerFragNUsedOffset     = 16
	headerFreeListOffset      = 20
	headerFreeFragListOffset  = 36
	headerFullFragListOffset  = 52
	headerSegIdOffset         = 68 // 次に割り当てる segment id
	headerSegInodesFullOffset = 76 // SEG_INODES_FULL リストの base node
	headerSegInodesFreeOffset = 92 // SEG_INODES_FREE リストの base node
	headerSize                = 108
)

// extentPageCount は 1 extent を構成するページ数 (= 1 MiB / ページサイズ)
const extentPageCount = (1 << 20) / page.Size

// freeAddExtents は fill 1 回で FREE リストへ追加する extent 数
const freeAddExtents = 4

// segFillReservedExtents はフリーリスト先読みを行う予約総ページ数の閾値 (extent 数)
const segFillReservedExtents = 40

// segFillAddExtents はフリーリスト先読み 1 回で segment の FREE リストへ追加する最大 extent 数
const segFillAddExtents = 4

const (
	// descriptorPageStride は記述子ページの配置間隔 (ページ数)
	descriptorPageStride = page.Size
	// descriptorEntriesPerPage は記述子ページ 1 枚に並ぶ xdes エントリ数
	descriptorEntriesPerPage = descriptorPageStride / extentPageCount
)

// xdesBitmapSize は free bit を並べた bitmap のバイト数 (= 1 bit/ページ × extent 内ページ数)
const xdesBitmapSize = extentPageCount / 8

// xdes エントリ (計 56 バイト) 内のオフセット (エントリ先頭からの相対値)
const (
	xdesIdOffset       = 0 // 所属 segment の id (segment 非帰属では 0)
	xdesFlstNodeOffset = 8
	xdesStateOffset    = 20
	xdesBitmapOffset   = 24
	xdesEntrySize      = xdesBitmapOffset + xdesBitmapSize
)

// inode ページ (body 先頭からの相対値)
const (
	inodePageNodeOffset = 0  // SEG_INODES リスト連結用 node (12 バイト)
	inodeArrOffset      = 12 // inode エントリ配列の開始位置
)

// inodeMagicValue は使用中の inode エントリに書かれる破損検出用マジックナンバー
const inodeMagicValue = "SEGI"

// inode エントリ (エントリ先頭からの相対値、計 576 バイト)
const (
	inodeSegIdOffset        = 0
	inodeNotFullNUsedOffset = 8
	inodeFreeListOffset     = 12
	inodeNotFullListOffset  = 28
	inodeFullListOffset     = 44
	inodeMagicOffset        = 60
	inodeFragArrOffset      = 64
	inodeFragSlotCount      = extentPageCount / 2
	inodeEntrySize          = inodeFragArrOffset + 4*inodeFragSlotCount
	inodeEntriesPerPage     = (page.Size - page.HeaderSize - inodeArrOffset) / inodeEntrySize
)

// descriptorPageNumber は pageNumber を担当する記述子ページの PageNumber を返す
func descriptorPageNumber(pageNumber page.PageNumber) page.PageNumber {
	return pageNumber / descriptorPageStride * descriptorPageStride
}

// descriptorEntryIndex は pageNumber を担当する記述子ページ内のエントリインデックスを返す
func descriptorEntryIndex(pageNumber page.PageNumber) int {
	return int(pageNumber%descriptorPageStride) / extentPageCount
}
