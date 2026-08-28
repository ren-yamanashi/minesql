package access

import (
	"errors"

	"github.com/ren-yamanashi/minesql/internal/storage/btree"
	"github.com/ren-yamanashi/minesql/internal/storage/buffer"
	"github.com/ren-yamanashi/minesql/internal/storage/lock"
)

// CurrentReadIterator は Current Read でプライマリインデックスを走査するイテレータ
//   - 各行に排他ロックを取得しながら最新バージョンを返す
//   - 取得したロックはトランザクション終了まで保持される (削除済み行にもロックを取得してからスキップする)
type CurrentReadIterator struct {
	iterator     *btree.ScanIterator
	primaryIndex *primaryIndex
	trxId        lock.TrxId
}

func newCurrentReadIterator(pi *primaryIndex, trxId lock.TrxId, iter *btree.ScanIterator) *CurrentReadIterator {
	return &CurrentReadIterator{iterator: iter, primaryIndex: pi, trxId: trxId}
}

// Next は次の行の最新バージョンを排他ロック付きで返す
//   - 最新バージョンが削除済みの行はスキップする (取得済みのロックはトランザクション終了まで保持される)
//   - ok=false は走査終端
//   - エラーを返した後のイテレータは再利用できない。Close を呼んで破棄する
func (ci *CurrentReadIterator) Next() (*PrimaryRecord, bool, error) {
	for {
		rec, mtr, ok, err := ci.iterator.Next()
		if err != nil {
			return nil, false, err
		}
		if !ok {
			return nil, false, nil
		}

		key := rec.Key()
		rowKey := lock.RowKey{MetaPageId: ci.primaryIndex.tree.MetaPageId(), Key: key}
		granted, handle := ci.primaryIndex.lock.TryLock(ci.trxId, rowKey, lock.Exclusive)

		if granted {
			deleteMark := rec.Header()[0]
			mtr.UnpinAll()
			if deleteMark == 1 {
				continue
			}
			decoded, err := DecodePrimaryRecord(rec, ci.primaryIndex.catalog, ci.primaryIndex.bufferPool, ci.primaryIndex.fileId())
			if err != nil {
				return nil, false, err
			}
			return decoded, true, nil
		}

		mtr.UnpinAll()
		if err := handle.Wait(); err != nil {
			return nil, false, err
		}

		decoded, found, err := ci.refetchLocked(key)
		if err != nil {
			return nil, false, err
		}
		if !found {
			continue
		}
		return decoded, true, nil
	}
}

// Close は走査を途中で打ち切るときに呼ぶ (終端到達時は自動解放されるため省略可)
func (ci *CurrentReadIterator) Close() {
	ci.iterator.Close()
}

// refetchLocked はロック取得後にキーで完全一致再検索を行い、最新バージョンをデコードして返す
//   - キーが存在しない、または最新バージョンが削除済みの場合は found=false を返す
func (ci *CurrentReadIterator) refetchLocked(key []byte) (*PrimaryRecord, bool, error) {
	mtr := buffer.NewMtr(ci.primaryIndex.bufferPool)
	defer mtr.UnpinAll()

	record, _, err := ci.primaryIndex.tree.FindByKey(mtr, key)
	if errors.Is(err, btree.ErrKeyNotFound) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, err
	}
	if record.Header()[0] == 1 {
		return nil, false, nil
	}
	decoded, err := DecodePrimaryRecord(record, ci.primaryIndex.catalog, ci.primaryIndex.bufferPool, ci.primaryIndex.fileId())
	if err != nil {
		return nil, false, err
	}
	return decoded, true, nil
}
