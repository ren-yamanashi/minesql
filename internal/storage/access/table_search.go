package access

import (
	"fmt"

	"github.com/ren-yamanashi/minesql/internal/storage/buffer"
	"github.com/ren-yamanashi/minesql/internal/storage/lock"
)

// Search は指定したトランザクションでプライマリインデックスを検索する
//   - trxMgr から ReadView を取得 (REPEATABLE READ のため最初の呼び出し時に作成しキャッシュ)
//   - 返ったイテレータは Consistent Read を実行する
func (t *Table) Search(trxMgr *TrxManager, trxId lock.TrxId, mtr *buffer.Mtr, mode SearchMode) (*PrimaryIndexIterator, error) {
	readView := trxMgr.CreateReadView(trxId)
	return t.primaryIndex.search(mtr, mode, readView)
}

// SearchSecondary は指定したセカンダリインデックスを検索する
//   - 指定したインデックス名が存在しない場合はエラー
//   - 可視性判定はプライマリ側に伝搬される (詳細はセカンダリイテレータを参照)
func (t *Table) SearchSecondary(trxMgr *TrxManager, trxId lock.TrxId, mtr *buffer.Mtr, indexName string, mode SearchMode) (*SecondaryIndexIterator, error) {
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
	readView := trxMgr.CreateReadView(trxId)
	return target.search(mtr, mode, readView)
}
