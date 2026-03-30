package btree

import (
	"minesql/internal/storage/btree/node"
	"minesql/internal/storage/buffer"
)

// Iterator は B+Tree のリーフノード (双方向連結リスト) を走査する
//
// 全件スキャンや範囲検索などで、リーフノードを順番に走査するために使用する
type Iterator struct {
	bufferPage buffer.BufferPage // 現在参照しているバッファページ
	slotNum    int               // 現在参照されているスロット番号 (slotted page のスロット番号)
}

// newIterator は指定されたバッファページとスロット番号を持つイテレータを生成する
//
// bufferPage: イテレータが参照するバッファページ
//
// slotNum: イテレータが参照するスロット番号 (slotted page のスロット番号)
func newIterator(bufferPage buffer.BufferPage, slotNum int) *Iterator {
	return &Iterator{
		bufferPage: bufferPage,
		slotNum:    slotNum,
	}
}

// Get は現在参照しているリーフノード (=バッファページ) のレコードを取得
func (iter *Iterator) Get() (node.Record, bool) {
	leafNode := node.NewLeafNode(iter.bufferPage.GetReadData())

	if iter.slotNum < leafNode.NumRecords() {
		record := leafNode.RecordAt(iter.slotNum)
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
func (iter *Iterator) Next(bp *buffer.BufferPool) (node.Record, bool, error) {
	record, ok := iter.Get()
	if !ok {
		return node.NewRecord(nil, nil, nil), false, nil
	}
	err := iter.Advance(bp)
	if err != nil {
		return node.NewRecord(nil, nil, nil), false, err
	}
	return record, true, nil
}

// Advance は次のレコードに進む
func (iter *Iterator) Advance(bp *buffer.BufferPool) error {
	iter.slotNum++

	leafNode := node.NewLeafNode(iter.bufferPage.GetReadData())

	// 現在のページ内に、まだ次のレコードがある場合は、次のページに移動しない (スロット番号を進めるだけ)
	if iter.slotNum < leafNode.NumRecords() {
		return nil
	}

	nextPageId := leafNode.NextPageId()

	// 現在のページ内に、次のレコードがないが、次のページも存在しない場合は、次のページに移動しない
	if nextPageId == nil {
		return nil
	}

	// 現在のページ内に次のレコードがなく、次のページが存在する場合は、次のページに移動する
	// 古いページの参照ビットをクリア
	oldPageId := iter.bufferPage.PageId
	bp.UnRefPage(oldPageId)

	// 次のページを取得
	buffer, err := bp.FetchPage(*nextPageId)
	if err != nil {
		return err
	}

	// イテレータを更新
	iter.bufferPage = *buffer
	iter.slotNum = 0

	return nil
}
