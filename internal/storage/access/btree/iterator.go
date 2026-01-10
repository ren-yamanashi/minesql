package btree

import (
	"minesql/internal/storage/access/btree/node"
	"minesql/internal/storage/bufferpool"
)

type Iterator struct {
	bufferPage bufferpool.BufferPage
	bufferId   int
}

func newIterator(bufferPage bufferpool.BufferPage, bufferId int) *Iterator {
	return &Iterator{
		bufferPage: bufferPage,
		bufferId:   bufferId,
	}
}

// 現在の key-value のペアを取得
func (iter *Iterator) Get() (node.Pair, bool) {
	_node := node.NewNode(iter.bufferPage.Page[:])
	leafNode := node.NewLeafNode(_node.Body())

	if iter.bufferId < leafNode.NumPairs() {
		pair := leafNode.PairAt(iter.bufferId)
		key := make([]byte, len(pair.Key))
		copy(key, pair.Key)
		value := make([]byte, len(pair.Value))
		copy(value, pair.Value)
		return node.NewPair(key, value), true
	}

	return node.Pair{}, false
}

// 次の key-value ペアに進む
func (iter *Iterator) Advance(bpm *bufferpool.BufferPoolManager) error {
	iter.bufferId++

	_node := node.NewNode(iter.bufferPage.Page[:])
	leafNode := node.NewLeafNode(_node.Body())

	// 現在のページ内に、まだ次の key-value ペアがある場合は、何もせずに終了
	if iter.bufferId < leafNode.NumPairs() {
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
	iter.bufferId = 0

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
