package disk

import "fmt"

const PAGE_SIZE = 4096

var (
	ErrInvalidDataSize error  = fmt.Errorf("data size must be %d bytes", PAGE_SIZE)
	INVALID_PAGE_ID    PageId = NewPageId(0xFFFFFFFF, 0xFFFFFFFF)
)

type Page [PAGE_SIZE]uint8

type PageId struct {
	// ディスクファイルの識別し
	FileId uint32
	// ファイル内のページ番号
	PageNumber uint32
}

func NewPageId(fileId uint32, pageNumber uint32) PageId {
	return PageId{
		FileId:     fileId,
		PageNumber: pageNumber,
	}
}

func (p *PageId) Equals(other PageId) bool {
	return p.FileId == other.FileId && p.PageNumber == other.PageNumber
}

func (p *PageId) IsInvalid() bool {
	return p.Equals(INVALID_PAGE_ID)
}

// +++++++++++++++++++++++
// OLD
// +++++++++++++++++++++++

const OLD_INVALID_PAGE_ID = OldPageId(0xFFFFFFFFFFFFFFFF) // 無効なページIDを表す定数 (`0xFFFFFFFFFFFFFFFF` は `uint64` の最大値を表す)
type OldPageId uint64
