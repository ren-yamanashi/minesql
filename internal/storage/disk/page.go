package disk

import "fmt"

const (
	PAGE_SIZE = 4096
	INVALID_PAGE_ID = PageId(0xFFFFFFFFFFFFFFFF) // 無効なページIDを表す定数 (`0xFFFFFFFFFFFFFFFF` は `uint64` の最大値を表す)
)

type PageId uint64

type Page [PAGE_SIZE]uint8

var ErrInvalidDataSize = fmt.Errorf("data size must be %d bytes", PAGE_SIZE)
