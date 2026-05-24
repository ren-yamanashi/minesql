package executor

import "github.com/ren-yamanashi/minesql/internal/storage/access"

// Union は複数の RowIterator の結果を結合し、重複を除去する
type Union struct {
	current        int // 現在利用中のイテレータのインデックス
	seen           map[string]struct{}
	innerIterators []RowIterator
}

func NewUnion(inners []RowIterator) *Union {
	return &Union{
		innerIterators: inners,
		seen:           make(map[string]struct{}),
	}
}

func (u *Union) Next() (access.Record, bool, error) {
	for u.current < len(u.innerIterators) {
		record, ok, err := u.innerIterators[u.current].Next()
		if err != nil {
			return nil, false, err
		}
		if !ok {
			u.current++
			continue
		}
		key := string(record.Encode().Bytes())
		if _, exists := u.seen[key]; exists {
			continue
		}
		u.seen[key] = struct{}{}
		return record, true, nil
	}
	return nil, false, nil
}
