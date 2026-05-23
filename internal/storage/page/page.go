package page

import (
	"errors"
	"strconv"
)

const (
	PageSize       = 4096
	PageHeaderSize = 4
)

var ErrInvalidDataSize = errors.New("data size must be " + strconv.Itoa(PageSize) + " bytes")

// Page は 4KB のページ
//   - ヘッダー: 先頭 4 バイト
//   - ボディ: 残りバイト
type Page struct {
	data   []byte
	Header []byte
	Body   []byte
}

func NewPage(data []byte) (*Page, error) {
	if err := CheckPageSize(data); err != nil {
		return nil, err
	}
	return &Page{
		data:   data,
		Header: data[:PageHeaderSize], //nolint:gosec // CheckPageSize で len(data) == PageSize を検証済み
		Body:   data[PageHeaderSize:], //nolint:gosec // CheckPageSize で len(data) == PageSize を検証済み
	}, nil
}

// ToBytes はページ全体のバイト列を返す
func (p *Page) ToBytes() []byte {
	return p.data
}

// CheckPageSize は data が 4KB であるかを確認する
func CheckPageSize(data []byte) error {
	if len(data) != PageSize {
		return ErrInvalidDataSize
	}
	return nil
}
