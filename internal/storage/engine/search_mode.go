package engine

import "minesql/internal/storage/access"

// SearchMode はテーブル/インデックスの検索方法を表す
type SearchMode interface {
	isSearchMode()
}

// SearchModeStart は先頭から検索する
type SearchModeStart struct{}

func (SearchModeStart) isSearchMode() {}

// SearchModeKey は指定したキーから検索する
type SearchModeKey struct {
	Key [][]byte
}

func (SearchModeKey) isSearchMode() {}

// TableIterator はテーブルのレコードを走査するイテレータ
type TableIterator interface {
	// Next はデコード済みの次のレコードを返す (DeleteMark 済みレコードはスキップ)
	Next() ([][]byte, bool, error)
}

func toAccessSearchMode(mode SearchMode) access.RecordSearchMode {
	switch m := mode.(type) {
	case SearchModeStart:
		return access.RecordSearchModeStart{}
	case SearchModeKey:
		return access.RecordSearchModeKey{Key: m.Key}
	default:
		panic("unknown search mode")
	}
}
