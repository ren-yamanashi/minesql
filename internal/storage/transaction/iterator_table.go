package transaction

import (
	"minesql/internal/storage/btree"
	"minesql/internal/storage/buffer"
	"minesql/internal/storage/encode"
	"minesql/internal/storage/lock"
)

type TableIterator struct {
	iterator *btree.Iterator
	bp       *buffer.BufferPool
	trxId    lock.TrxId
	lockMgr  *lock.Manager
}

func newTableIterator(iterator *btree.Iterator, bp *buffer.BufferPool, trxId lock.TrxId, lockMgr *lock.Manager) *TableIterator {
	return &TableIterator{
		iterator: iterator,
		bp:       bp,
		trxId:    trxId,
		lockMgr:  lockMgr,
	}
}

// Next はデコード済みの次のレコードを返す
// (DeleteMark が設定されているレコードはスキップする)
//
// 各行の読み取り時に共有ロックを取得する
//
// 戻り値: レコード (プライマリキー + 値), データがあるかどうか, エラー
func (ri *TableIterator) Next() ([][]byte, bool, error) {
	for {
		btrRecord, ok, err := ri.iterator.Next(ri.bp)
		if !ok {
			return nil, false, nil
		}
		if err != nil {
			return nil, false, err
		}

		// DeleteMark が 1 のレコードはスキップ
		if len(btrRecord.HeaderBytes()) > 0 && btrRecord.HeaderBytes()[0] == 1 {
			continue
		}

		// 共有ロックを取得
		if err := ri.lockMgr.Lock(ri.trxId, ri.iterator.LastPosition, lock.Shared); err != nil {
			return nil, false, err
		}

		// レコード (プライマリキー + NonKey) をデコード
		var record [][]byte
		encode.Decode(btrRecord.KeyBytes(), &record)
		encode.Decode(btrRecord.NonKeyBytes(), &record)

		return record, true, nil
	}
}
