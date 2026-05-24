package access

import "github.com/ren-yamanashi/minesql/internal/storage/btree"

var (
	_ Record = (*PrimaryRecord)(nil)
	_ Record = (*SecondaryRecord)(nil)
)

type Record interface {
	Encode() btree.Record
}
