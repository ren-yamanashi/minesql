package btree

import (
	"minesql/internal/storage/access/btree/node"
	"minesql/internal/storage/bufferpool"
)

type Iterator struct {
	bufferPage bufferpool.BufferPage
	// 現在参照されているスロット番号 (slotted page のスロット番号)
	slotNum   int
}

func newIterator(bufferPage bufferpool.BufferPage, slotNum int) *Iterator {
	return &Iterator{
		bufferPage: bufferPage,
		slotNum:   slotNum,
	}
}

// 現在の key-value のペアを取得
func (iter *Iterator) Get() (node.Pair, bool) {
	leafNode := node.NewLeafNode(iter.bufferPage.GetReadData())

	if iter.slotNum < leafNode.NumPairs() {
		pair := leafNode.PairAt(iter.slotNum)
		key := make([]byte, len(pair.Key))
		copy(key, pair.Key)
		value := make([]byte, len(pair.Value))
		copy(value, pair.Value)
		return node.NewPair(key, value), true
	}

	return node.NewPair(nil, nil), false
}

// 次の key-value ペアに進む
func (iter *Iterator) Advance(bpm *bufferpool.BufferPoolManager) error {
	iter.slotNum++

	leafNode := node.NewLeafNode(iter.bufferPage.GetReadData())

	// 現在のページ内に、まだ次の key-value ペアがある場合は、何もせずに終了
	if iter.slotNum < leafNode.NumPairs() {
		return nil
	}

	nextPageId := leafNode.NextPageId()

	// 現在のページ内に、次の key-value ペアがないが、次のページも存在しない場合は何もしない
	if nextPageId == nil {
		return nil
	}

	// 現在のページ内に次の key-value ペアがなく、次のページが存在する場合は、次のページに移動する
	// 古いページの参照ビットをクリア
	oldPageId := iter.bufferPage.PageId
	bpm.UnRefPage(oldPageId)

	// 次のページを取得
	buffer, err := bpm.FetchPage(*nextPageId)
	if err != nil {
		return err
	}

	// イテレータを更新
	iter.bufferPage = *buffer
	iter.slotNum = 0

	return nil
}

// 次の key-value のペアを取得する
func (iter *Iterator) Next(bpm *bufferpool.BufferPoolManager) (node.Pair, bool, error) {
	pair, ok := iter.Get()
	if !ok {
		return node.NewPair(nil, nil), false, nil
	}
	err := iter.Advance(bpm)
	if err != nil {
		return node.NewPair(nil, nil), false, err
	}
	return pair, true, nil
}
