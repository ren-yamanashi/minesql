package page

import "encoding/binary"

const MaxFileId = 0xFFFFFFFF
const MaxPageNumber = 0xFFFFFFFF

var InvalidPageId = NewPageId(MaxFileId, MaxPageNumber)

type (
	FileId     uint32 // ディスクファイルの識別子
	PageNumber uint32 // ファイル内のページ番号
)

// PageId はディスクファイルとファイル内のページを特定するための識別子
type PageId struct {
	FileId     FileId     // ディスクファイルの識別子
	PageNumber PageNumber // ファイル内のページ番号
}

// NewPageId は FileId と PageNumber から PageId を生成する
func NewPageId(fileId FileId, pageNumber PageNumber) PageId {
	return PageId{
		FileId:     fileId,
		PageNumber: pageNumber,
	}
}

// IsInvalid はこの PageId が無効な PageID (InvalidPageId) かどうかを判定する
func (p *PageId) IsInvalid() bool {
	return *p == InvalidPageId
}

// WriteTo は PageId を指定位置に書き込む
func (p *PageId) WriteTo(data []byte, offset int) {
	binary.BigEndian.PutUint32(data[offset:offset+4], uint32(p.FileId))
	binary.BigEndian.PutUint32(data[offset+4:offset+8], uint32(p.PageNumber))
}

// ToBytes は PageId をバイト列に変換する
//   - 先頭4バイト: FileId
//   - 次の4バイト: PageNumber
func (p *PageId) ToBytes() []byte {
	buf := make([]byte, 8)
	p.WriteTo(buf, 0)
	return buf
}

// RestorePageIdFromBytes はバイト列から PageId を復元する
//   - data: PageId を表す8バイトのバイト列 (先頭4バイトに FileId、次の4バイトに PageNumber が格納されている必要がある)
func RestorePageIdFromBytes(data []byte) PageId {
	if len(data) != 8 {
		panic("data size must be 8 bytes to convert to PageId")
	}
	return ReadPageIdFromPageData(data, 0)
}

// ReadPageIdFromPageData はページデータから PageId を読み取る
//   - data: ページデータ全体
//   - offset: PageId が格納されている位置 (通常はページの先頭、つまり offset=0)
func ReadPageIdFromPageData(data []byte, offset int) PageId {
	return PageId{
		FileId:     FileId(binary.BigEndian.Uint32(data[offset : offset+4])),
		PageNumber: PageNumber(binary.BigEndian.Uint32(data[offset+4 : offset+8])),
	}
}
