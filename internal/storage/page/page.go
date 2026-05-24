package page

import (
	"errors"
	"fmt"
	"strconv"
)

const (
	Size       = 4 * 1024 // 4KB
	HeaderSize = 4
)

var ErrInvalidDataSize = errors.New("data size must be " + strconv.Itoa(Size) + " bytes")

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
		Header: data[:HeaderSize], //nolint:gosec // CheckPageSize で len(data) == PageSize を検証済み
		Body:   data[HeaderSize:], //nolint:gosec // CheckPageSize で len(data) == PageSize を検証済み
	}, nil
}

// ToBytes はページ全体のバイト列を返す
func (p *Page) ToBytes() []byte {
	return p.data
}

// Copy はページのデータを新しいメモリ領域にコピーした Page を返す
//
// pg が nil データの場合はゼロ値の Page を返す
func Copy(pg Page) Page {
	if pg.data == nil {
		return Page{}
	}
	copied := make([]byte, Size)
	copy(copied, pg.ToBytes())
	p, err := NewPage(copied) // make([]byte, Size) でサイズ保証済みのため、ここでのエラーは不変条件違反を意味するので panic で良い
	if err != nil {
		panic(fmt.Sprintf("page: Copy failed: %v", err))
	}
	return *p
}

// IsZero は Page がゼロ値かどうかを判定する
func (p *Page) IsZero() bool {
	return p.data == nil
}

// CheckPageSize は data が 4KB であるかを確認する
func CheckPageSize(data []byte) error {
	if len(data) != Size {
		return ErrInvalidDataSize
	}
	return nil
}
