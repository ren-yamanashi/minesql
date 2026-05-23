package btree

import (
	"bytes"

	"github.com/ren-yamanashi/minesql/internal/storage/buffer"
)

// Iterator は B+Tree のリーフノードを走査する
type Iterator struct {
	bufferPool *buffer.Pool
	bufferPage buffer.Page // 現在参照しているバッファページ
	slotNum    int         // 現在参照されているスロット番号
}

func NewIterator(bufPool *buffer.Pool, bufPage buffer.Page, slotNum int) *Iterator {
	return &Iterator{
		bufferPool: bufPool,
		bufferPage: bufPage,
		slotNum:    slotNum,
	}
}

// Close はイテレータが保持しているバッファページの参照を解放する
func (it *Iterator) Close() {
	it.bufferPool.UnrefPage(it.bufferPage.PageId())
}

// Get は現在参照しているリーフノードのレコードを取得
func (it *Iterator) Get() (Record, bool, error) {
	pg, err := it.bufferPool.PageForRead(it.bufferPage.PageId())
	if err != nil {
		return NewRecord(nil, nil, nil), false, err
	}
	leaf := newLeafNode(pg.Data())

	if it.slotNum < leaf.numRecords() {
		record := leaf.record(it.slotNum)
		header := bytes.Clone(record.Header())
		key := bytes.Clone(record.Key())
		nonKey := bytes.Clone(record.NonKey())
		return NewRecord(header, key, nonKey), true, nil
	}
	return NewRecord(nil, nil, nil), false, nil
}

// Next は次のレコードを取得する
func (it *Iterator) Next() (Record, bool, error) {
	record, ok, err := it.Get()
	if err != nil {
		return nil, false, err
	}
	if !ok {
		return NewRecord(nil, nil, nil), false, nil
	}

	err = it.Advance()
	if err != nil {
		return NewRecord(nil, nil, nil), false, err
	}
	return record, true, nil
}

// Advance は次のレコードに進む
func (it *Iterator) Advance() error {
	pg, err := it.bufferPool.PageForRead(it.bufferPage.PageId())
	if err != nil {
		return err
	}
	leaf := newLeafNode(pg.Data())

	// 現在のページ内に、次のレコードがある場合
	if it.slotNum < leaf.numRecords() {
		it.slotNum++
	}

	// まだ現在のページ内にレコードがある場合
	if it.slotNum < leaf.numRecords() {
		return nil
	}

	// 現在のページのレコードを全て読み終えた場合
	nextPageId := leaf.nextPageId()

	// 次のページがなければ何もしない
	if nextPageId.IsInvalid() {
		return nil
	}

	// 次のページに移動
	oldPageId := it.bufferPage.PageId()
	it.bufferPool.UnrefPage(oldPageId)
	nextPage, err := it.bufferPool.PageForRead(nextPageId)
	if err != nil {
		return err
	}

	it.bufferPage = *nextPage
	it.slotNum = 0
	return nil
}
