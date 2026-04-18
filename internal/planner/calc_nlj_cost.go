package planner

import (
	"errors"
	"minesql/internal/storage/access"
	"minesql/internal/storage/btree"
	"minesql/internal/storage/buffer"
	"minesql/internal/storage/handler"
)

// joinCandidate は結合候補のテーブル情報
type joinCandidate struct {
	tblMeta *handler.TableMetadata
	stats   *handler.TableStatistics
	table   *access.Table
}

// joinPredicate は 2 テーブル間の結合条件 (ON 句から抽出)
//
// INNER JOIN は可換なので、ON 条件は特定テーブルに属さず、テーブルペアの関係として管理する
type joinPredicate struct {
	leftTable  string
	leftCol    string
	rightTable string
	rightCol   string
}

// optimizeJoinOrder は貪欲法で最小コストの結合順序を返す
func optimizeJoinOrder(
	bp *buffer.BufferPool,
	candidates []joinCandidate,
	predicates []joinPredicate,
) ([]joinCandidate, error) {
	remaining := make([]joinCandidate, len(candidates))
	copy(remaining, candidates)

	var result []joinCandidate
	resultTableNames := make(map[string]struct{})
	prefixRowcount := 1.0
	prefixCost := 0.0

	for len(remaining) > 0 {
		bestIdx := -1
		bestCost := 0.0
		bestFanout := 0.0

		for i, candidate := range remaining {
			// 2 回目以降: 結果セット内のテーブルとの結合条件が必要
			var pred *joinPredicate
			if len(resultTableNames) > 0 {
				pred = findPredicate(predicates, candidate.tblMeta.Name, resultTableNames)
				if pred == nil {
					continue
				}
			}

			readCost, fanout, err := calcTableJoinCost(bp, candidate, prefixRowcount, pred)
			if err != nil {
				return nil, err
			}
			newPrefixRowcount := prefixRowcount * fanout
			newPrefixCost := prefixCost + readCost + newPrefixRowcount*RowEvaluateCost

			if bestIdx == -1 || newPrefixCost < bestCost {
				bestIdx = i
				bestCost = newPrefixCost
				bestFanout = fanout
			}
		}

		if bestIdx == -1 {
			return nil, errors.New("no valid join order: unreachable table in remaining candidates")
		}

		result = append(result, remaining[bestIdx])
		resultTableNames[remaining[bestIdx].tblMeta.Name] = struct{}{}
		prefixCost = bestCost
		prefixRowcount *= bestFanout

		// remaining から bestIdx を削除
		remaining = append(remaining[:bestIdx], remaining[bestIdx+1:]...)
	}

	return result, nil
}

// calcTableJoinCost はテーブルの結合コスト (readCost と fanout) を算出する
//
// pred が nil の場合は駆動表 (フルスキャン)
// pred が non-nil の場合は内部表 (eq_ref またはフルスキャン)
func calcTableJoinCost(
	bp *buffer.BufferPool,
	candidate joinCandidate,
	prefixRowcount float64,
	pred *joinPredicate,
) (readCost float64, fanout float64, err error) {
	primaryBTree := btree.NewBTree(candidate.table.MetaPageId)
	pageReadCost, err := calcPageReadCost(bp, primaryBTree)
	if err != nil {
		return 0, 0, err
	}

	// 駆動表: フルスキャン
	if pred == nil {
		scanTime := float64(candidate.stats.LeafPageCount)
		return scanTime * pageReadCost, float64(candidate.stats.RecordCount), nil
	}

	// 内部表: 結合カラムからアクセス方法を判定
	joinColName := resolveJoinCol(pred, candidate.tblMeta.Name)

	// 単一カラム PK で eq_ref (複合 PK は先頭カラムだけでは一意にならないため対象外)
	if candidate.tblMeta.PKCount == 1 {
		colMeta, ok := candidate.tblMeta.GetColByName(joinColName)
		if ok && colMeta.Pos == 0 {
			return prefixRowcount * pageReadCost, 1.0, nil
		}
	}

	// UNIQUE INDEX で eq_ref
	idxMeta, hasIndex := candidate.tblMeta.GetIndexByColName(joinColName)
	if hasIndex {
		idxStats, ok := candidate.stats.IdxStats[idxMeta.Name]
		if ok {
			return prefixRowcount * pageReadCost, idxStats.RecPerKey, nil
		}
	}

	// インデックスなし: フルスキャン
	scanTime := float64(candidate.stats.LeafPageCount)
	return prefixRowcount * scanTime * pageReadCost, float64(candidate.stats.RecordCount), nil
}

// findPredicate は predicates から候補テーブルと結果セット内のテーブルを結ぶ条件を探す
//
// INNER JOIN は可換なので、predicate の leftTable/rightTable のどちら側が候補でもマッチする
func findPredicate(predicates []joinPredicate, candidateTable string, resultTableNames map[string]struct{}) *joinPredicate {
	for i := range predicates {
		pred := &predicates[i]
		if pred.leftTable == candidateTable {
			if _, ok := resultTableNames[pred.rightTable]; ok {
				return pred
			}
		}
		if pred.rightTable == candidateTable {
			if _, ok := resultTableNames[pred.leftTable]; ok {
				return pred
			}
		}
	}
	return nil
}

// resolveJoinCol は joinPredicate から候補テーブル側の結合カラム名を返す
func resolveJoinCol(pred *joinPredicate, candidateTable string) string {
	if pred.leftTable == candidateTable {
		return pred.leftCol
	}
	return pred.rightCol
}
