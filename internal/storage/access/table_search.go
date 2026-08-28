package access

import (
	"fmt"
)

// Search は指定したトランザクションでプライマリインデックスを検索する
//   - 返るイテレータが降下用の mini-transaction と各レコード取得の短期 mini-transaction を内部管理する
func (t *Table) Search(trx *Transaction, mode SearchMode) (*PrimaryIndexIterator, error) {
	readView := trx.tm.EnsureReadView(trx)
	return t.primaryIndex.search(mode, readView)
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
//   - 返るイテレータが降下用の mini-transaction と各レコード取得の短期 mini-transaction を内部管理する
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
	readView := trx.tm.EnsureReadView(trx)
	return target.search(mode, readView)
}
