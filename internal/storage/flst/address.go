package flst

import (
	"encoding/binary"

	"github.com/ren-yamanashi/minesql/internal/storage/page"
)

// addressSize は Address のエンコードサイズ (PageNumber 4 バイト + Offset 2 バイト)
const addressSize = 6

// Address はページ上のリストノードの位置
//   - Offset はページ body 先頭からの相対オフセット
type Address struct {
	PageNumber page.PageNumber
	Offset     uint16
}

// InvalidAddress は無効を示す Address を返す
func InvalidAddress() Address {
	return Address{PageNumber: page.MaxPageNumber}
}

// IsInvalid は無効な Address かどうかを返す
func (a Address) IsInvalid() bool {
	return a.PageNumber == page.MaxPageNumber
}

// WriteAt は Address を指定位置に 6 バイトで書き込む
//   - data: データ全体
//   - offset: 書き込み開始位置
func (a Address) WriteAt(data []byte, offset int) {
	binary.BigEndian.PutUint32(data[offset:offset+4], uint32(a.PageNumber))
	binary.BigEndian.PutUint16(data[offset+4:offset+addressSize], a.Offset)
}

// ReadAddress は Address を指定位置から読み込む
//   - data: データ全体
//   - offset: Address が格納されている位置
func ReadAddress(data []byte, offset int) Address {
	return Address{
		PageNumber: page.PageNumber(binary.BigEndian.Uint32(data[offset : offset+4])),
		Offset:     binary.BigEndian.Uint16(data[offset+4 : offset+addressSize]),
	}
}
