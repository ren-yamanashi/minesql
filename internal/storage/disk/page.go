package disk

import "fmt"

const PAGE_SIZE = 4096

type PageId uint64

var ErrInvalidDataSize = fmt.Errorf("data size must be %d bytes", PAGE_SIZE)
