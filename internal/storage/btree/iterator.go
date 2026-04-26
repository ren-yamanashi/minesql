package btree

import (
	"github.com/ren-yamanashi/minesql/internal/storage/btree/node"
	"github.com/ren-yamanashi/minesql/internal/storage/buffer"
	"github.com/ren-yamanashi/minesql/internal/storage/page"
)

// Iterator は B+Tree のリーフノード (双方向連結リスト) を走査する
//
// 全件スキャンや範囲検索などで、リーフノードを順番に走査するために使用する
type Iterator struct {
	bufferPool   *buffer.BufferPool
	bufferPage   buffer.BufferPage // 現在参照しているバッファページ
	slotNum      int               // 現在参照されているスロット番号 (slotted page のスロット番号)
	LastPosition page.SlotPosition // 直前に Next で返したレコードの位置
}

// newIterator は指定されたバッファページとスロット番号を持つイテレータを生成する
//   - bufferPool: バッファプール
//   - bufferPage: イテレータが参照するバッファページ
//   - slotNum: イテレータが参照するスロット番号 (slotted page のスロット番号)
func newIterator(bufferPool *buffer.BufferPool, bufferPage buffer.BufferPage, slotNum int) *Iterator {
	return &Iterator{
		bufferPool: bufferPool,
		bufferPage: bufferPage,
		slotNum:    slotNum,
	}
}

// Get は現在参照しているリーフノード (=バッファページ) のレコードを取得
func (iter *Iterator) Get() (node.Record, bool) {
	data, err := iter.bufferPool.GetReadPageData(iter.bufferPage.PageId)
	if err != nil {
		return node.NewRecord(nil, nil, nil), false
	}
	leaf := node.NewLeaf(page.NewPage(data).Body)

	if iter.slotNum < leaf.NumRecords() {
		record := leaf.RecordAt(iter.slotNum)
		header := make([]byte, len(record.HeaderBytes()))
		copy(header, record.HeaderBytes())
		key := make([]byte, len(record.KeyBytes()))
		copy(key, record.KeyBytes())
		nonKey := make([]byte, len(record.NonKeyBytes()))
		copy(nonKey, record.NonKeyBytes())
		return node.NewRecord(header, key, nonKey), true
	}

	return node.NewRecord(nil, nil, nil), false
}

// Next は次のレコードを取得する
func (iter *Iterator) Next(bufferPool *buffer.BufferPool) (node.Record, bool, error) {
	iter.LastPosition = page.SlotPosition{
		PageId:  iter.bufferPage.PageId,
		SlotNum: iter.slotNum,
	}
	record, ok := iter.Get()
	if !ok {
		return node.NewRecord(nil, nil, nil), false, nil
	}
	err := iter.Advance(bufferPool)
	if err != nil {
		return node.NewRecord(nil, nil, nil), false, err
	}
	return record, true, nil
}

// Advance は次のレコードに進む
func (iter *Iterator) Advance(bufferPool *buffer.BufferPool) error {
	iter.slotNum++

	data, err := iter.bufferPool.GetReadPageData(iter.bufferPage.PageId)
	if err != nil {
		return err
	}
	leaf := node.NewLeaf(page.NewPage(data).Body)

	// 現在のページ内に、まだ次のレコードがある場合は、次のページに移動しない (スロット番号を進めるだけ)
	if iter.slotNum < leaf.NumRecords() {
		return nil
	}

	nextPageId := leaf.NextPageId()

	// 現在のページ内に、次のレコードがないが、次のページも存在しない場合は、次のページに移動しない
	if nextPageId == nil {
		return nil
	}

	// 現在のページ内に次のレコードがなく、次のページが存在する場合は、次のページに移動する
	// 古いページの参照ビットをクリア
	oldPageId := iter.bufferPage.PageId
	bufferPool.UnRefPage(oldPageId)

	// 次のページを取得
	nextPage, err := bufferPool.FetchPage(*nextPageId)
	if err != nil {
		return err
	}

	// イテレータを更新
	iter.bufferPage = *nextPage
	iter.slotNum = 0

	return nil
}
