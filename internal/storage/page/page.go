package page

import (
	"errors"
	"fmt"
)

const (
	Size       = 4 * 1024 // 4KB
	HeaderSize = 4
)

var ErrInvalidDataSize = errors.New("invalid page data size")

// Page は 4KB のページ
//   - ヘッダー: 先頭 4 バイト
//   - ボディ: 残りバイト
type Page struct {
	data   []byte
	header []byte
	body   []byte
}

func NewPage(data []byte) (*Page, error) {
	if err := CheckSize(data); err != nil {
		return nil, err
	}
	return &Page{
		data:   data,
		header: data[:HeaderSize], //nolint:gosec // CheckSize で len(data) == Size を検証済み
		body:   data[HeaderSize:], //nolint:gosec // CheckSize で len(data) == Size を検証済み
	}, nil
}

func (p *Page) Header() []byte { return p.header }
func (p *Page) Body() []byte   { return p.body }
func (p *Page) Bytes() []byte  { return p.data }

// Copy はページのデータを新しいメモリ領域にコピーした Page を返す
func (p *Page) Copy() *Page {
	if p == nil || p.IsZero() {
		return &Page{}
	}
	copied := make([]byte, Size)
	copy(copied, p.data)
	// make([]byte, Size) でサイズ保証済みのため、ここでのエラーは不変条件違反を意味するので panic で良い
	newPage, err := NewPage(copied)
	if err != nil {
		panic(fmt.Sprintf("page: Copy failed: %v", err))
	}
	return newPage
}

// IsZero は Page がゼロ値かどうかを判定する
func (p *Page) IsZero() bool {
	return p == nil || p.data == nil
}

// CheckSize は data が Size バイトであるかを確認する
func CheckSize(data []byte) error {
	if len(data) != Size {
		return fmt.Errorf("%w: must be %d bytes, got %d", ErrInvalidDataSize, Size, len(data))
	}
	return nil
}
