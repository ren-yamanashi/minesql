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
	if err := CheckPageSize(data); err != nil {
		return nil, err
	}
	return &Page{
		Header: data[:PageHeaderSize],
		Body:   data[PageHeaderSize:],
	}, nil
}

// CheckPageSize は data が 16KB であるかを確認する
func CheckPageSize(data []byte) error {
	if len(data) != PageSize {
		return ErrInvalidDataSize
	}
	return nil
}
