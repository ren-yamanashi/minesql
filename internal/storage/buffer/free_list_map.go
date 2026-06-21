package buffer

import (
	"encoding/binary"

	"github.com/ren-yamanashi/minesql/internal/storage/page"
)

const (
	freeListMapEntryCountOffset = 0
	freeListMapEntryCountSize   = 4
	freeListMapEntriesOffset    = 4
	freeListMapEntrySize        = 8 // FileId (4) + PageNumber (4)
)

// InitializeFreeListMapPage はマップページの内容を「エントリ 0 件」にして書き込む
//   - 呼び出し側は対象ページを X ラッチで保持している前提
func InitializeFreeListMapPage(bufPage *Page) {
	newFreeListMapPage(bufPage).initialize()
}

type freeListMapPage struct {
	data    *page.Page
	bufPage *Page
}

func newFreeListMapPage(bufPage *Page) *freeListMapPage {
	return &freeListMapPage{data: bufPage.Data(), bufPage: bufPage}
}

func (m *freeListMapPage) initialize() {
	var buf [freeListMapEntryCountSize]byte
	binary.BigEndian.PutUint32(buf[:], 0)
	m.bufPage.WriteBodyAt(freeListMapEntryCountOffset, buf[:])
}

func (m *freeListMapPage) entryCount() uint32 {
	return binary.BigEndian.Uint32(
		m.data.Body()[freeListMapEntryCountOffset : freeListMapEntryCountOffset+freeListMapEntryCountSize],
	)
}

// headPageNumber は指定 FileId のフリーリスト先頭 PageNumber を返す
//   - 該当 FileId のエントリが無い場合は無効値 (page.MaxPageNumber) を返す
func (m *freeListMapPage) headPageNumber(fileId page.FileId) page.PageNumber {
	body := m.data.Body()
	for i := range int(m.entryCount()) {
		offset := freeListMapEntriesOffset + i*freeListMapEntrySize
		entryFileId := page.FileId(binary.BigEndian.Uint32(body[offset : offset+4]))
		if entryFileId != fileId {
			continue
		}
		return page.PageNumber(binary.BigEndian.Uint32(body[offset+4 : offset+8]))
	}
	return page.MaxPageNumber
}

// setHeadPageNumber は指定 FileId のフリーリスト先頭 PageNumber を更新する
//   - 該当 FileId のエントリが既にある場合: 上書き
//   - 該当 FileId のエントリが無い場合: 末尾に追加
func (m *freeListMapPage) setHeadPageNumber(fileId page.FileId, pn page.PageNumber) {
	body := m.data.Body()
	count := m.entryCount()
	for i := range int(count) {
		offset := freeListMapEntriesOffset + i*freeListMapEntrySize
		entryFileId := page.FileId(binary.BigEndian.Uint32(body[offset : offset+4]))
		if entryFileId != fileId {
			continue
		}
		var buf [4]byte
		binary.BigEndian.PutUint32(buf[:], uint32(pn))
		m.bufPage.WriteBodyAt(offset+4, buf[:])
		return
	}

	offset := freeListMapEntriesOffset + int(count)*freeListMapEntrySize
	var entryBuf [freeListMapEntrySize]byte
	binary.BigEndian.PutUint32(entryBuf[0:4], uint32(fileId))
	binary.BigEndian.PutUint32(entryBuf[4:8], uint32(pn))
	m.bufPage.WriteBodyAt(offset, entryBuf[:])

	var countBuf [freeListMapEntryCountSize]byte
	binary.BigEndian.PutUint32(countBuf[:], count+1)
	m.bufPage.WriteBodyAt(freeListMapEntryCountOffset, countBuf[:])
}
