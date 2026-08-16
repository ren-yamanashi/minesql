package fsp

import (
	"testing"

	"github.com/ren-yamanashi/minesql/internal/storage/page"
	"github.com/stretchr/testify/assert"
)

func TestLayoutConstants(t *testing.T) {
	t.Run("FSP ヘッダーのレイアウト定数", func(t *testing.T) {
		// THEN
		assert.Equal(t, "MINE", magicValue)
		assert.Equal(t, 0, headerMagicOffset)
		assert.Equal(t, 4, headerFileIdOffset)
		assert.Equal(t, 8, headerSizeOffset)
		assert.Equal(t, 12, headerFreeLimitOffset)
		assert.Equal(t, 16, headerFragNUsedOffset)
		assert.Equal(t, 20, headerFreeListOffset)
		assert.Equal(t, 36, headerFreeFragListOffset)
		assert.Equal(t, 52, headerFullFragListOffset)
		assert.Equal(t, 68, headerSegIdOffset)
		assert.Equal(t, 76, headerSegInodesFullOffset)
		assert.Equal(t, 92, headerSegInodesFreeOffset)
		assert.Equal(t, 108, headerSize)
	})

	t.Run("extent と記述子ページの定数", func(t *testing.T) {
		// THEN
		assert.Equal(t, 256, extentPageCount)
		assert.Equal(t, 4096, descriptorPageStride)
		assert.Equal(t, 16, descriptorEntriesPerPage)
		assert.Equal(t, 4, freeAddExtents)
	})

	t.Run("xdes エントリのレイアウト定数", func(t *testing.T) {
		// THEN
		assert.Equal(t, 0, xdesIdOffset)
		assert.Equal(t, 8, xdesFlstNodeOffset)
		assert.Equal(t, 20, xdesStateOffset)
		assert.Equal(t, 24, xdesBitmapOffset)
		assert.Equal(t, 56, xdesEntrySize)
		assert.Equal(t, 32, xdesBitmapSize)
	})
}

func TestDescriptorPageNumber(t *testing.T) {
	tests := []struct {
		name       string
		pageNumber page.PageNumber
		want       page.PageNumber
	}{
		{"page 0 は記述子ページ 0", 0, 0},
		{"page 255 は記述子ページ 0", 255, 0},
		{"page 256 は記述子ページ 0", 256, 0},
		{"page 4095 は記述子ページ 0", 4095, 0},
		{"page 4096 は記述子ページ 4096", 4096, 4096},
		{"page 4097 は記述子ページ 4096", 4097, 4096},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// WHEN
			got := descriptorPageNumber(tt.pageNumber)

			// THEN
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestDescriptorEntryIndex(t *testing.T) {
	tests := []struct {
		name       string
		pageNumber page.PageNumber
		want       int
	}{
		{"page 0 はエントリ 0", 0, 0},
		{"page 255 はエントリ 0", 255, 0},
		{"page 256 はエントリ 1", 256, 1},
		{"page 4095 はエントリ 15", 4095, 15},
		{"page 4096 はエントリ 0", 4096, 0},
		{"page 4097 はエントリ 0", 4097, 0},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// WHEN
			got := descriptorEntryIndex(tt.pageNumber)

			// THEN
			assert.Equal(t, tt.want, got)
		})
	}
}
