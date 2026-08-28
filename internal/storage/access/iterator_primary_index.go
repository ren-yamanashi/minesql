package access

import (
	"github.com/ren-yamanashi/minesql/internal/storage/btree"
	"github.com/ren-yamanashi/minesql/internal/storage/buffer"
	"github.com/ren-yamanashi/minesql/internal/storage/dictionary"
	"github.com/ren-yamanashi/minesql/internal/storage/page"
	"github.com/ren-yamanashi/minesql/internal/storage/undo"
)

type PrimaryIndexIterator struct {
	iterator   *btree.ScanIterator
	catalog    *dictionary.Catalog
	bufferPool *buffer.Pool
	fileId     page.FileId
	readView   *readView     // nil 可。nil の場合は可視性判定をスキップし deleteMark のみで判定する
	undoLog    *undo.Manager // readView が非 nil のとき必須
}

func NewPrimaryIndexIterator(
	iter *btree.ScanIterator,
	ct *dictionary.Catalog,
	bp *buffer.Pool,
	fileId page.FileId,
	readView *readView,
	undoLog *undo.Manager,
) *PrimaryIndexIterator {
	return &PrimaryIndexIterator{
		fileId:     fileId,
		catalog:    ct,
		bufferPool: bp,
		iterator:   iter,
		readView:   readView,
		undoLog:    undoLog,
	}
}

// Close は走査を途中で打ち切るときに呼ぶ (終端到達時は自動解放されるため省略可)
func (pi *PrimaryIndexIterator) Close() {
	pi.iterator.Close()
}

// Next はデコード済みの次の可視レコードを返す
//   - readView が nil の場合は deleteMark のみで判定する従来動作
//   - readView が非 nil の場合は MVCC の可視性判定 + Undo 遡及を行う
func (pi *PrimaryIndexIterator) Next() (*PrimaryRecord, bool, error) {
	for {
		rec, mtr, ok, err := pi.iterator.Next()
		if err != nil {
			return nil, false, err
		}
		if !ok {
			return nil, false, nil
		}
		result, done, err := pi.resolveOne(rec, mtr)
		mtr.UnpinAll()
		if err != nil {
			return nil, false, err
		}
		if done {
			return result, true, nil
		}
	}
}

// resolveOne は 1 レコードについて可視性判定・Undo 遡及を行う
//   - 戻り値 done=true のとき result が可視レコード、done=false のときは continue して次のレコードへ
func (pi *PrimaryIndexIterator) resolveOne(record btree.Record, mtr *buffer.Mtr) (*PrimaryRecord, bool, error) {
	if pi.readView == nil && record.Header()[0] == 1 {
		return nil, false, nil
	}
	current, err := DecodePrimaryRecord(record, pi.catalog, pi.bufferPool, pi.fileId)
	if err != nil {
		return nil, false, err
	}
	if pi.readView == nil {
		return current, true, nil
	}

	visible, err := pi.resolveVisible(current, mtr)
	if err != nil {
		return nil, false, err
	}
	if visible == nil {
		return nil, false, nil
	}
	return visible, true, nil
}

// resolveVisible は可視性判定と Undo チェーン遡及を行い、Read View から見える行を返す
//   - 可視 (deleteMark=0)  → そのバージョン
//   - 可視 (deleteMark=1)  → nil (削除済み)
//   - チェーン終端まで遡って見つからない → nil
func (pi *PrimaryIndexIterator) resolveVisible(record *PrimaryRecord, mtr *buffer.Mtr) (*PrimaryRecord, error) {
	current := record
	for {
		// 不可視 → Undo を辿って次のバージョンへ
		if !pi.readView.isVisible(current.lastTrxId) {
			prev, err := resolvePrevVersion(resolvePrevVersionInput{
				mtr:        mtr,
				undoLog:    pi.undoLog,
				catalog:    pi.catalog,
				bufferPool: pi.bufferPool,
				fileId:     pi.fileId,
				record:     current,
			})
			if err != nil {
				return nil, err
			}
			if prev == nil {
				return nil, nil //nolint:nilnil // nil は「Read View からは存在しない」を表す
			}
			current = prev
			continue
		}

		// 可視
		if current.deleteMark == 1 {
			return nil, nil //nolint:nilnil // nil は「Read View からは存在しない」を表す
		}
		return current, nil
	}
}
