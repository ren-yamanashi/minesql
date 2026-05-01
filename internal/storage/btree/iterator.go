package btree

import (
	"bytes"

	"github.com/ren-yamanashi/minesql/internal/storage/btree/node"
	"github.com/ren-yamanashi/minesql/internal/storage/buffer"
)

// Iterator は B+Tree のリーフノードを走査する
type Iterator struct {
	bufferPool   *buffer.BufferPool
	bufferPage   buffer.BufferPage   // 現在参照しているバッファページ
	slotNum      int                 // 現在参照されているスロット番号
	LastPosition node.RecordPosition // 直前に Next で取得されたレコードの位置
}

func NewIterator(bufPool *buffer.BufferPool, bufPage buffer.BufferPage, slotNum int) *Iterator {
	return &Iterator{
		bufferPool: bufPool,
		bufferPage: bufPage,
		slotNum:    slotNum,
	}
}

// Get は現在参照しているリーフノードのレコードを取得
func (iter *Iterator) Get() (node.Record, bool, error) {
	pg, err := iter.bufferPool.GetReadPage(iter.bufferPage.PageId)
	if err != nil {
		return node.NewRecord(nil, nil, nil), false, err
	}
	leaf := node.NewLeafNode(pg.Body)

	if iter.slotNum < leaf.NumRecords() {
		record := leaf.Record(iter.slotNum)
		header := bytes.Clone(record.Header())
		key := bytes.Clone(record.Key())
		nonKey := bytes.Clone(record.NonKey())
		return node.NewRecord(header, key, nonKey), true, nil
	}
	return node.NewRecord(nil, nil, nil), false, nil
}

// Next は次のレコードを取得する
func (iter *Iterator) Next() (node.Record, bool, error) {
	iter.LastPosition = node.RecordPosition{
		PageId:  iter.bufferPage.PageId,
		SlotNum: iter.slotNum,
	}

	record, ok, err := iter.Get()
	if err != nil {
		return nil, false, err
	}
	if !ok {
		return node.NewRecord(nil, nil, nil), false, nil
	}

	err = iter.Advance()
	if err != nil {
		return node.NewRecord(nil, nil, nil), false, err
	}
	return record, true, nil
}

// Advance は次のレコードに進む
func (iter *Iterator) Advance() error {
	pg, err := iter.bufferPool.GetReadPage(iter.bufferPage.PageId)
	if err != nil {
		return err
	}
	leaf := node.NewLeafNode(pg.Body)

	// 現在のページ内に、次のレコードがある場合
	if iter.slotNum < leaf.NumRecords() {
		iter.slotNum++
		return nil
	}

	// 次のレコードが無い場合
	nextPageId := leaf.NextPageId()

	// 次のページがなければ何もしない
	if nextPageId.IsInvalid() {
		return nil
	}

	// 次のページに移動
	oldPageId := iter.bufferPage.PageId
	iter.bufferPool.UnRefPage(oldPageId)
	nextPage, err := iter.bufferPool.FetchPage(nextPageId)
	if err != nil {
		return err
	}

	iter.bufferPage = *nextPage
	iter.slotNum = 0
	return nil
}
