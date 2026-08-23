package undo

import (
	"encoding/binary"
	"fmt"

	"github.com/ren-yamanashi/minesql/internal/storage/buffer"
	"github.com/ren-yamanashi/minesql/internal/storage/flst"
	"github.com/ren-yamanashi/minesql/internal/storage/fsp"
	"github.com/ren-yamanashi/minesql/internal/storage/page"
)

const (
	headerUsedBytesOffset        = 0
	headerNextPageNumberOffset   = 2
	pageHeaderSize               = 6  // UsedBytes(2) + NextPageNum(4)
	firstPageSegmentHeaderOffset = 6  // チェーン先頭ページの segment header 開始オフセット
	firstPageHeaderSize          = 12 // UsedBytes(2) + NextPageNum(4) + segment header(6)
	// maxRecordSize は 1 つの Undo レコードが単一ページ (後続ページのボディ) に収まる最大バイト数
	maxRecordSize = page.Size - page.HeaderSize - pageHeaderSize
)

// ChainHeadPageNumber は Undo チェーンの先頭ページの PageNumber
//   - Undo ファイルの bootstrap 順序 (page 0 = FSP ヘッダー、 page 1 = 最初の inode ページ、 page 2 = チェーン先頭) の帰結
const ChainHeadPageNumber = page.PageNumber(2)

type Page struct {
	// header / body は bufPage.Data().Body() への読み取りビュー (書き込みは bufPage の API 経由で行う必要がある)
	// bodyStart はページボディ先頭から body slice の先頭までのバイト数 (後続ページ = 6、 先頭ページ = 12)
	header    []byte
	body      []byte
	bodyStart int
	bufPage   *buffer.Page
}

// NewPage は既存の Undo ページを開く (チェーンの後続ページ用)
func NewPage(bufPage *buffer.Page) *Page {
	body := bufPage.Data().Body()
	return &Page{
		header:    body[:pageHeaderSize],
		body:      body[pageHeaderSize:],
		bodyStart: pageHeaderSize,
		bufPage:   bufPage,
	}
}

// CreatePage は新規 Undo ページを作成する (チェーンの後続ページ用)
func CreatePage(bufPage *buffer.Page) *Page {
	p := NewPage(bufPage)
	p.initialize()
	return p
}

// NewFirstPage は Undo チェーンの先頭ページを開く
//   - 先頭ページのボディは body offset 6 に segment header 領域が予約され、 レコード領域はその後ろから始まる
func NewFirstPage(bufPage *buffer.Page) *Page {
	body := bufPage.Data().Body()
	return &Page{
		header:    body[:pageHeaderSize],
		body:      body[firstPageHeaderSize:],
		bodyStart: firstPageHeaderSize,
		bufPage:   bufPage,
	}
}

// CreateFirstPage は Undo チェーンの先頭ページを新規初期化する
//   - segment header 領域 (offset 6-11) には書き込まない
func CreateFirstPage(bufPage *buffer.Page) *Page {
	p := NewFirstPage(bufPage)
	p.initialize()
	return p
}

// BodyAt はボディの [offset, offset+length) のバイト範囲を返す
//   - 範囲がボディサイズを超える場合は nil を返す
//   - 戻り値はボディの内部 slice なので、 書き込みは行わず読み取り専用で扱うこと
func (p *Page) BodyAt(offset, length int) []byte {
	if offset < 0 || length < 0 || offset+length > len(p.body) {
		return nil
	}
	return p.body[offset : offset+length]
}

// Record はボティ内の指定 offset のレコードを読み取る
func (p *Page) Record(offset int) []byte {
	if offset >= len(p.body) {
		return nil
	}
	if offset+recordHeaderSize > len(p.body) {
		return nil
	}
	dataLen := int(binary.BigEndian.Uint16(p.body[offset+headerDataLenOffset : offset+recordHeaderSize]))
	totalLen := recordHeaderSize + dataLen
	if offset+totalLen > len(p.body) {
		return nil
	}
	return p.body[offset : offset+totalLen]
}

// UsedBytes はボディの使用済みバイト数を返す
func (p *Page) UsedBytes() uint16 {
	return binary.BigEndian.Uint16(p.header[headerUsedBytesOffset:headerNextPageNumberOffset])
}

// NextPageNumber は次の Undo ページの PageNumber を返す
func (p *Page) NextPageNumber() page.PageNumber {
	return page.PageNumber(binary.BigEndian.Uint32(p.header[headerNextPageNumberOffset:pageHeaderSize]))
}

// FreeSpace はボディ内の空き容量を返す
func (p *Page) FreeSpace() int {
	return len(p.body) - int(p.UsedBytes())
}

// initialize は Undo ページを初期化する
func (p *Page) initialize() {
	var buf [pageHeaderSize]byte
	binary.BigEndian.PutUint16(buf[headerUsedBytesOffset:headerNextPageNumberOffset], 0)
	binary.BigEndian.PutUint32(buf[headerNextPageNumberOffset:pageHeaderSize], 0)
	p.bufPage.WriteBodyAt(0, buf[:])
}

// append は Undo レコードをボディに追加する
//
// 空き不足の場合は false を返す
func (p *Page) append(record []byte) bool {
	used := int(p.UsedBytes())
	if used+len(record) > len(p.body) {
		return false
	}
	p.bufPage.WriteBodyAt(p.bodyStart+used, record)
	p.setUsedBytes(uint16(used + len(record)))
	return true
}

// setNextPageNumber は次の UNDO ページの PageNumber を設定する
func (p *Page) setNextPageNumber(pn page.PageNumber) {
	var buf [4]byte
	binary.BigEndian.PutUint32(buf[:], uint32(pn))
	p.bufPage.WriteBodyAt(headerNextPageNumberOffset, buf[:])
}

// setUsedBytes は使用済みバイト数を設定する
func (p *Page) setUsedBytes(n uint16) {
	var buf [2]byte
	binary.BigEndian.PutUint16(buf[:], n)
	p.bufPage.WriteBodyAt(headerUsedBytesOffset, buf[:])
}

// CreateChainRoot は Undo チェーン全体を管理する segment を新規作成し、 チェーン先頭ページの PageId を返す
//   - mtr: segment 作成と先頭ページ初期化を記録する Mtr。 Commit / UnpinAll は呼び出し側
//   - fileId: Undo チェーンを配置するファイルの FileId
//   - bootstrap 経路のため、途中失敗はすべて panic で扱う
func CreateChainRoot(mtr *buffer.Mtr, fileId page.FileId) page.Id {
	pageId, err := fsp.CreateSegment(mtr, fileId, firstPageSegmentHeaderOffset)
	if err != nil {
		panic(fmt.Sprintf("undo: bootstrap failed to create chain root segment: %v", err))
	}
	bufPage, err := mtr.PageForWrite(pageId)
	if err != nil {
		panic(fmt.Sprintf("undo: bootstrap failed to acquire chain root page: %v", err))
	}
	CreateFirstPage(bufPage)
	return pageId
}

// chainRootHeaderAt はチェーン先頭ページ上の segment header のアドレスを返す
func chainRootHeaderAt(rootPageId page.Id) flst.Address {
	return flst.Address{PageNumber: rootPageId.PageNumber(), Offset: firstPageSegmentHeaderOffset}
}
