package access

import (
	"github.com/ren-yamanashi/minesql/internal/storage/btree"
	"github.com/ren-yamanashi/minesql/internal/storage/buffer"
	"github.com/ren-yamanashi/minesql/internal/storage/dictionary"
	"github.com/ren-yamanashi/minesql/internal/storage/page"
	"github.com/ren-yamanashi/minesql/internal/storage/undo"
)

type PrimaryIndexIterator struct {
	iterator   *btree.Iterator
	catalog    *dictionary.Catalog
	bufferPool *buffer.Pool
	fileId     page.FileId
	readView   *readView     // nil 可。nil の場合は可視性判定をスキップし deleteMark のみで判定する
	undoLog    *undo.Manager // readView が非 nil のとき必須
	mtr        *buffer.Mtr   // readView が非 nil のとき必須 (Undo ページ取得用)
}

func NewPrimaryIndexIterator(
	iter *btree.Iterator,
	ct *dictionary.Catalog,
	bp *buffer.Pool,
	fileId page.FileId,
	readView *readView,
	undoLog *undo.Manager,
	mtr *buffer.Mtr,
) *PrimaryIndexIterator {
	return &PrimaryIndexIterator{
		fileId:     fileId,
		catalog:    ct,
		bufferPool: bp,
		iterator:   iter,
		readView:   readView,
		undoLog:    undoLog,
		mtr:        mtr,
	}
}

func (pi *PrimaryIndexIterator) Close() {
	pi.iterator.Close()
}

// Next はデコード済みの次の可視レコードを返す
//   - readView が nil の場合は deleteMark のみで判定する従来動作
//   - readView が非 nil の場合は MVCC の可視性判定 + Undo 遡及を行う
func (pi *PrimaryIndexIterator) Next() (*PrimaryRecord, bool, error) {
	for {
		record, ok, err := pi.iterator.Next()
		if err != nil {
			return nil, false, err
		}
		if !ok {
			return nil, false, nil
		}

		// readView=nil の場合は deleteMark のみで判定 (Undo 遡及なし)
		if pi.readView == nil && record.Header()[0] == 1 {
			continue
		}

		current, err := DecodePrimaryRecord(record, pi.catalog, pi.bufferPool, pi.fileId)
		if err != nil {
			return nil, false, err
		}
		if pi.readView == nil {
			return current, true, nil
		}

		visible, err := pi.resolveVisible(current)
		if err != nil {
			return nil, false, err
		}
		if visible == nil {
			continue
		}
		return visible, true, nil
	}
}

// resolveVisible は可視性判定と Undo チェーン遡及を行い、Read View から見える行を返す
//   - 可視 (deleteMark=0)  → そのバージョン
//   - 可視 (deleteMark=1)  → nil (削除済み)
//   - チェーン終端まで遡って見つからない → nil
func (pi *PrimaryIndexIterator) resolveVisible(record *PrimaryRecord) (*PrimaryRecord, error) {
	current := record
	for {
		// 不可視 → Undo を辿って次のバージョンへ
		if !pi.readView.isVisible(current.lastTrxId) {
			prev, err := resolvePrevVersion(resolvePrevVersionInput{
				mtr:        pi.mtr,
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
