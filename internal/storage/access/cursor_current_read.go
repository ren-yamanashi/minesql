package access

import (
	"errors"

	"github.com/ren-yamanashi/minesql/internal/storage/btree"
	"github.com/ren-yamanashi/minesql/internal/storage/buffer"
	"github.com/ren-yamanashi/minesql/internal/storage/lock"
)

// currentReadCursor は UPDATE/DELETE の対象行を Current Read で取得するカーソル
//   - Read View を参照せず、プライマリインデックス上の最新バージョンを排他ロック付きで読む
//   - ロック待ちが必要な場合は B+Tree のラッチを保持しない状態で待ち、獲得後にキーで再検索する
type currentReadCursor struct {
	pi    *primaryIndex
	trxId lock.TrxId
}

func newCurrentReadCursor(pi *primaryIndex, trxId lock.TrxId) *currentReadCursor {
	return &currentReadCursor{pi: pi, trxId: trxId}
}

// lockLatest は mode で最初に到達したレコードの最新バージョンを排他ロック付きで返す
//   - found=false は対象行が存在しない、または削除済みであることを表す
func (c *currentReadCursor) lockLatest(mode SearchMode) (*PrimaryRecord, bool, error) {
	mtr := buffer.NewMtr(c.pi.bufferPool)
	defer mtr.UnpinAll()

	// 降下して対象レコードのキーを得る (ラッチ・Pin は解放済みで返る)
	// この時点ではロック未取得のため、ここで読んだ値は確定値として扱わない
	record, found, err := c.firstRecord(mtr, mode)
	if err != nil || !found {
		return nil, false, err
	}
	key := record.Key()

	// 排他ロックを原子的に即時判定する
	rowKey := lock.RowKey{MetaPageId: c.pi.tree.MetaPageId(), Key: key}
	granted, handle := c.pi.lock.TryLock(c.trxId, rowKey, lock.Exclusive)
	if !granted {
		// firstRecord はラッチ・Pin を解放済みなので、ラッチ非保持で待機できる
		if err := handle.Wait(); err != nil {
			return nil, false, err
		}
	}

	// ロック取得後にキーで再検索し、ロック保持下で読んだ最新バージョンを採用する
	// (即時付与でも、ロック取得前に読んだ firstRecord の値はロック取得までの間に他トランザクションが更新・コミットしている可能性があるため、必ずロック取得後に読み直す)
	locked, found, err := c.findByKey(mtr, key)
	if err != nil || !found {
		return nil, false, err
	}

	// 最新バージョンが削除済みなら対象なし
	if locked.Header()[0] == 1 {
		return nil, false, nil
	}
	decoded, err := DecodePrimaryRecord(locked, c.pi.catalog, c.pi.bufferPool, c.pi.fileId())
	if err != nil {
		return nil, false, err
	}
	return decoded, true, nil
}

// firstRecord は mode で降下して最初のレコードを取得する
//   - 戻り値はロック対象のキーを得るために使う (値の確定はロック取得後の findByKey で行う)
//   - レコードのバイト列は複製済みで返るため、ラッチ・Pin を解放した後も参照できる
func (c *currentReadCursor) firstRecord(mtr *buffer.Mtr, mode SearchMode) (btree.Record, bool, error) {
	iter, err := c.pi.tree.Search(mtr, mode.Encode())
	if err != nil {
		return nil, false, err
	}
	return iter.Get()
}

// findByKey は指定キーの最新レコードを取得する (見つからなければ found=false)
func (c *currentReadCursor) findByKey(mtr *buffer.Mtr, key []byte) (btree.Record, bool, error) {
	record, _, err := c.pi.tree.FindByKey(mtr, key)
	if errors.Is(err, btree.ErrKeyNotFound) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, err
	}
	return record, true, nil
}
