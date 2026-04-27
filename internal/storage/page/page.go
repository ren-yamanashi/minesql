package page

import "fmt"

const PageSize = 16384
const PageHeaderSize = 4

var ErrInvalidDataSize = fmt.Errorf("data size must be %d bytes", PageSize)

// Page は 16KB のページ
//
// レイアウト:
//   - ヘッダー: 先頭 4 バイト
//   - ボディ: 残りバイト
type Page struct {
	Header []byte
	Body   []byte
}

func NewPage(data []byte) (*Page, error) {
	size := len(data)
	if size != PageSize {
		return nil, ErrInvalidDataSize
	}
	return &Page{
		Header: data[:PageHeaderSize],
		Body:   data[PageHeaderSize:],
	}, nil
}
