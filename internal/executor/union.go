package executor

import "encoding/binary"

// Union は複数の Executor の結果を結合し、重複を除去する
type Union struct {
	current        int
	seen           map[string]struct{}
	innerExecutors []Executor
}

func NewUnion(innerExecutors []Executor) *Union {
	return &Union{
		innerExecutors: innerExecutors,
		seen:           make(map[string]struct{}),
	}
}

func (u *Union) Next() (Record, error) {
	for u.current < len(u.innerExecutors) {
		record, err := u.innerExecutors[u.current].Next()
		if err != nil {
			return nil, err
		}
		if record == nil {
			u.current++
			continue
		}

		key := recordKey(record)
		if _, exists := u.seen[key]; exists {
			continue
		}
		u.seen[key] = struct{}{}
		return record, nil
	}
	return nil, nil
}

// recordKey はレコードから重複判定用のキーを生成する
//
// 各カラムの長さとデータを連結した文字列を返す
func recordKey(record Record) string {
	var buf []byte
	for _, col := range record {
		buf = binary.AppendUvarint(buf, uint64(len(col)))
		buf = append(buf, col...)
	}
	return string(buf)
}
