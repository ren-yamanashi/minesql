package page

import (
	"fmt"
)

const (
	PageSize       = 4096
	PageHeaderSize = 4
)

var ErrInvalidDataSize = fmt.Errorf("data size must be %d bytes", PageSize)

// Page は 4KB のページ
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
		Header: data[:PageHeaderSize], //nolint:gosec // CheckPageSize で len(data) == PageSize を検証済み
		Body:   data[PageHeaderSize:], //nolint:gosec // CheckPageSize で len(data) == PageSize を検証済み
	}, nil
}

// ToBytes はページ全体のバイト列を返す
func (p Page) ToBytes() []byte {
	return p.Header[:PageSize]
}

// CheckPageSize は data が 4KB であるかを確認する
func CheckPageSize(data []byte) error {
	if len(data) != PageSize {
		return ErrInvalidDataSize
	}
	return nil
}
