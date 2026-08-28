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

// SearchForUpdate は指定したトランザクションで Current Read の走査イテレータを返す
//   - 各行に排他ロックを取得しながら最新バージョンを返す (Read View は参照しない)
//   - 他のトランザクションが対象行に排他ロックを保持していれば行ごとにロック待ちになる
//   - 最新バージョンが削除済みの行はスキップされる (取得済みのロックはトランザクション終了まで保持される)
func (t *Table) SearchForUpdate(trx *Transaction, mode SearchMode) (*CurrentReadIterator, error) {
	return t.primaryIndex.searchForUpdate(mode, trx.trxId)
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
