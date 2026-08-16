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
	headerSegIdOffset         = 68 // セグメント管理のための予約 (0 固定)
	headerSegInodesFullOffset = 76 // セグメント管理のための予約 (0 固定)
	headerSegInodesFreeOffset = 92 // セグメント管理のための予約 (0 固定)
	headerSize                = 108
)

// extentPageCount は 1 extent を構成するページ数 (= 1 MiB / ページサイズ)
const extentPageCount = (1 << 20) / page.Size

// freeAddExtents は fill 1 回で FREE リストへ追加する extent 数
const freeAddExtents = 4

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
	xdesIdOffset       = 0 // セグメント管理のための予約 (0 固定)
	xdesFlstNodeOffset = 8
	xdesStateOffset    = 20
	xdesBitmapOffset   = 24
	xdesEntrySize      = xdesBitmapOffset + xdesBitmapSize
)

// descriptorPageNumber は pageNumber を担当する記述子ページの PageNumber を返す
func descriptorPageNumber(pageNumber page.PageNumber) page.PageNumber {
	return pageNumber / descriptorPageStride * descriptorPageStride
}

// descriptorEntryIndex は pageNumber を担当する記述子ページ内のエントリインデックスを返す
func descriptorEntryIndex(pageNumber page.PageNumber) int {
	return int(pageNumber%descriptorPageStride) / extentPageCount
}
