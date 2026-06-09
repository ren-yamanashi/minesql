package access

import (
	"fmt"

	"github.com/ren-yamanashi/minesql/internal/storage/buffer"
)

// Search は指定したトランザクションでプライマリインデックスを検索する
func (t *Table) Search(trx *Transaction, mtr *buffer.Mtr, mode SearchMode) (*PrimaryIndexIterator, error) {
	readView := trx.tm.EnsureReadView(trx)
	return t.primaryIndex.search(mtr, mode, readView)
}

// SearchSecondary は指定したセカンダリインデックスを検索する
//   - 指定したインデックス名が存在しない場合はエラー
//   - 可視性判定はプライマリ側に伝搬される (詳細はセカンダリイテレータを参照)
func (t *Table) SearchSecondary(trx *Transaction, mtr *buffer.Mtr, indexName string, mode SearchMode) (*SecondaryIndexIterator, error) {
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
	return target.search(mtr, mode, readView)
}
