package access

import (
	"fmt"

	"github.com/ren-yamanashi/minesql/internal/storage/buffer"
)

// Search は指定したトランザクションでプライマリインデックスを検索する
func (t *Table) Search(trx *Transaction, mode SearchMode) (*PrimaryIndexIterator, error) {
	mtr := buffer.NewMtr(t.bufferPool)
	defer mtr.UnpinAll()
	readView := trx.tm.EnsureReadView(trx)
	return t.primaryIndex.search(mtr, mode, readView)
}

// SearchForUpdate は UPDATE/DELETE の対象行を Current Read で取得する
//   - Read View を参照せず、プライマリインデックス上の最新バージョンを排他ロック付きで返す
//   - 他のトランザクションが対象行に排他ロックを保持していればロック待ちになる
//   - 対象行が存在しない、または削除済みの場合は found=false を返す
func (t *Table) SearchForUpdate(trx *Transaction, mode SearchMode) (*PrimaryRecord, bool, error) {
	cursor := newCurrentReadCursor(t.primaryIndex, trx.trxId)
	return cursor.lockLatest(mode)
}

// SearchSecondary は指定したセカンダリインデックスを検索する
//   - 指定したインデックス名が存在しない場合はエラー
//   - 可視性判定はプライマリ側に伝搬される (詳細はセカンダリイテレータを参照)
func (t *Table) SearchSecondary(trx *Transaction, indexName string, mode SearchMode) (*SecondaryIndexIterator, error) {
	var target *secondaryIndex
	for _, si := range t.secondaryIndexes {
		if si.indexName == indexName {
			target = si
			break
		}
	}
	if target == nil {
		return nil, fmt.Errorf("secondary index %q not found", indexName)
	}
	mtr := buffer.NewMtr(t.bufferPool)
	defer mtr.UnpinAll()
	readView := trx.tm.EnsureReadView(trx)
	return target.search(mtr, mode, readView)
}
