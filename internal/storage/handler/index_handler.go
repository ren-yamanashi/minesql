package handler

import (
	"minesql/internal/storage/access"
	"minesql/internal/storage/buffer"
)

// IndexHandler は access.UniqueIndexAccessMethod をラップし、ユニークインデックスへの操作を提供する
type IndexHandler struct {
	inner      *access.UniqueIndexAccessMethod
	tableInner *access.TableAccessMethod // Search 時にテーブル参照が必要
}

func (i *IndexHandler) Search(bp *buffer.BufferPool, mode SearchMode) (IndexIterator, error) {
	accessMode := toAccessSearchMode(mode)
	iter, err := i.inner.Search(bp, i.tableInner, accessMode)
	if err != nil {
		return nil, err
	}
	return &indexIteratorAdapter{inner: iter}, nil
}

func (i *IndexHandler) Create(bp *buffer.BufferPool) error {
	return i.inner.Create(bp)
}

func (i *IndexHandler) GetName() string {
	return i.inner.Name
}

func (i *IndexHandler) GetColName() string {
	return i.inner.ColName
}

// IndexSearchResult はインデックス検索の結果
type IndexSearchResult struct {
	SecondaryKey [][]byte // デコード済みセカンダリキー
	Record       [][]byte // デコード済みテーブルレコード (プライマリキー + 値)
}

// IndexIterator はインデックスの検索結果を走査するイテレータ
type IndexIterator interface {
	// Next はインデックスから次の検索結果を返す (DeleteMark 済みレコードはスキップ)
	Next() (*IndexSearchResult, bool, error)
}

// indexIteratorAdapter は access.SecondaryIndexIterator を IndexIterator に適合させる
// (SecondaryIndexSearchResult -> handler.IndexSearchResult の変換)
type indexIteratorAdapter struct {
	inner *access.UniqueIndexIterator
}

func (it *indexIteratorAdapter) Next() (*IndexSearchResult, bool, error) {
	result, ok, err := it.inner.Next()
	if !ok || err != nil {
		return nil, ok, err
	}
	return &IndexSearchResult{
		SecondaryKey: result.UniqueKey,
		Record:       result.Record,
	}, true, nil
}
