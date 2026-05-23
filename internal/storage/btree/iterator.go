package btree

import (
	"bytes"

	"github.com/ren-yamanashi/minesql/internal/storage/buffer"
)

// Iterator は B+Tree のリーフノードを走査する
type Iterator struct {
	bufferPool   *buffer.Pool
	bufferPage   buffer.Page    // 現在参照しているバッファページ
	slotNum      int            // 現在参照されているスロット番号
	lastPosition RecordPosition // 直前に Next で取得されたレコードの位置
}

func NewIterator(bufPool *buffer.Pool, bufPage buffer.Page, slotNum int) *Iterator {
	return &Iterator{
		bufferPool: bufPool,
		bufferPage: bufPage,
		slotNum:    slotNum,
	}
}

// Get は現在参照しているリーフノードのレコードを取得
func (iter *Iterator) Get() (Record, bool, error) {
	pg, err := iter.bufferPool.BufferPageForRead(iter.bufferPage.PageId)
	if err != nil {
		return NewRecord(nil, nil, nil), false, err
	}
	leaf := NewLeafNode(pg.Page)

	if iter.slotNum < leaf.NumRecords() {
		record := leaf.Record(iter.slotNum)
		header := bytes.Clone(record.Header())
		key := bytes.Clone(record.Key())
		nonKey := bytes.Clone(record.NonKey())
		return NewRecord(header, key, nonKey), true, nil
	}
	return NewRecord(nil, nil, nil), false, nil
}

// Next は次のレコードを取得する
func (iter *Iterator) Next() (Record, bool, error) {
	iter.lastPosition = RecordPosition{
		PageId:  iter.bufferPage.PageId,
		SlotNum: iter.slotNum,
	}

	record, ok, err := iter.Get()
	if err != nil {
		return nil, false, err
	}
	if !ok {
		return NewRecord(nil, nil, nil), false, nil
	}

	err = iter.Advance()
	if err != nil {
		return NewRecord(nil, nil, nil), false, err
	}
	return record, true, nil
}

// Advance は次のレコードに進む
func (iter *Iterator) Advance() error {
	pg, err := iter.bufferPool.BufferPageForRead(iter.bufferPage.PageId)
	if err != nil {
		return err
	}
	leaf := NewLeafNode(pg.Page)

	// 現在のページ内に、次のレコードがある場合
	if iter.slotNum < leaf.NumRecords() {
		iter.slotNum++
	}

	// まだ現在のページ内にレコードがある場合
	if iter.slotNum < leaf.NumRecords() {
		return nil
	}

	// 現在のページのレコードを全て読み終えた場合
	nextPageId := leaf.NextPageId()

	// 次のページがなければ何もしない
	if nextPageId.IsInvalid() {
		return nil
	}

	// 次のページに移動
	oldPageId := iter.bufferPage.PageId
	iter.bufferPool.UnRefPage(oldPageId)
	nextPage, err := iter.bufferPool.BufferPageForRead(nextPageId)
	if err != nil {
		return err
	}

	iter.bufferPage = *nextPage
	iter.slotNum = 0
	return nil
}
