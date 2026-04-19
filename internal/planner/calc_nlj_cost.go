package planner

import (
	"errors"
	"minesql/internal/ast"
	"minesql/internal/storage/access"
	"minesql/internal/storage/btree"
	"minesql/internal/storage/buffer"
	"minesql/internal/storage/dictionary"
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
	where *ast.WhereClause,
	allTables []*handler.TableMetadata,
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

			// 駆動表候補の場合、WHERE 条件のうちこの候補に関係する条件を抽出
			var drivingWhere *ast.WhereClause
			if pred == nil && where != nil {
				drivingWhere, _ = splitWhereForTable(where, candidate.tblMeta, allTables)
			}

			readCost, fanout, err := calcTableJoinCost(bp, candidate, prefixRowcount, pred, drivingWhere)
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

		bestCandidate := remaining[bestIdx]
		result = append(result, bestCandidate)
		resultTableNames[bestCandidate.tblMeta.Name] = struct{}{}
		prefixCost = bestCost
		prefixRowcount *= bestFanout

		// filtered: WHERE 条件のうちインデックスで絞り込めない条件の通過率を prefix_rowcount に反映
		if where != nil {
			tableWhere, _ := splitWhereForTable(where, bestCandidate.tblMeta, allTables)
			if tableWhere != nil {
				filtered := calcFiltered(tableWhere.Condition, bestCandidate)
				prefixRowcount *= filtered
			}
		}

		// remaining から bestIdx を削除
		remaining = append(remaining[:bestIdx], remaining[bestIdx+1:]...)
	}

	return result, nil
}

// calcTableJoinCost はテーブルの結合コスト (readCost と fanout) を算出する
//
// pred が nil の場合は駆動表。drivingWhere があればアクセスパスを最適化する
// pred が non-nil の場合は内部表 (eq_ref またはフルスキャン)
func calcTableJoinCost(
	bp *buffer.BufferPool,
	candidate joinCandidate,
	prefixRowcount float64,
	pred *joinPredicate,
	drivingWhere *ast.WhereClause,
) (readCost float64, fanout float64, err error) {
	primaryBTree := btree.NewBTree(candidate.table.MetaPageId)
	pageReadCost, err := calcPageReadCost(bp, primaryBTree)
	if err != nil {
		return 0, 0, err
	}

	// 駆動表
	if pred == nil {
		return calcDrivingTableCost(bp, candidate, pageReadCost, drivingWhere)
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

	// セカンダリインデックスで検索 (unique: eq_ref fanout=1, non-unique: ref fanout=RecPerKey)
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

// calcDrivingTableCost は駆動表のコストを算出する
//
// drivingWhere があれば以下の順でアクセスパスを評価する:
//  1. PK/UNIQUE INDEX の等値検索 → ユニークスキャンコスト (1.0)
//  2. PK/UNIQUE INDEX のレンジスキャン → RecordsInRange + rangeCost
//  3. フルスキャン (デフォルト)
func calcDrivingTableCost(bp *buffer.BufferPool, candidate joinCandidate, pageReadCost float64, drivingWhere *ast.WhereClause) (readCost float64, fanout float64, err error) {
	if drivingWhere != nil {
		cost, f, ok, err := evalDrivingWhereConditions(bp, candidate, pageReadCost, drivingWhere.Condition)
		if err != nil {
			return 0, 0, err
		}
		if ok {
			return cost, f, nil
		}
	}

	// デフォルト: フルスキャン
	scanTime := float64(candidate.stats.LeafPageCount)
	return scanTime * pageReadCost, float64(candidate.stats.RecordCount), nil
}

// evalDrivingWhereConditions は WHERE 条件からベストなアクセスパスのコストを評価する
//
// AND の場合は各リーフ条件を個別に評価し、最もコストの低いものを返す
func evalDrivingWhereConditions(bp *buffer.BufferPool, candidate joinCandidate, pageReadCost float64, expr *ast.BinaryExpr) (cost float64, fanout float64, ok bool, err error) {
	if expr == nil {
		return 0, 0, false, nil
	}

	// AND の場合: 左右を再帰して最安を選択
	if expr.Operator == "AND" {
		if lhsExpr, lOk := expr.Left.(*ast.LhsExpr); lOk {
			if rhsExpr, rOk := expr.Right.(*ast.RhsExpr); rOk {
				lCost, lFan, lOk, lErr := evalDrivingWhereConditions(bp, candidate, pageReadCost, lhsExpr.Expr)
				if lErr != nil {
					return 0, 0, false, lErr
				}
				rCost, rFan, rOk, rErr := evalDrivingWhereConditions(bp, candidate, pageReadCost, rhsExpr.Expr)
				if rErr != nil {
					return 0, 0, false, rErr
				}
				if lOk && rOk {
					if lCost < rCost {
						return lCost, lFan, true, nil
					}
					return rCost, rFan, true, nil
				}
				if lOk {
					return lCost, lFan, true, nil
				}
				if rOk {
					return rCost, rFan, true, nil
				}
				return 0, 0, false, nil
			}
		}
	}

	// リーフ条件: col op literal
	lhs, lOk := expr.Left.(*ast.LhsColumn)
	_, rOk := expr.Right.(*ast.RhsLiteral)
	if !lOk || !rOk {
		return 0, 0, false, nil
	}

	colName := lhs.Column.ColName

	// 等値検索: PK or INDEX
	if expr.Operator == "=" {
		if candidate.tblMeta.PKCount == 1 {
			if colMeta, exists := candidate.tblMeta.GetColByName(colName); exists && colMeta.Pos == 0 {
				return calcUniqueScanCost(), 1.0, true, nil
			}
		}
		if idxMeta, hasIdx := candidate.tblMeta.GetIndexByColName(colName); hasIdx {
			if idxMeta.Type == dictionary.IndexTypeUnique {
				return calcUniqueScanCost(), 1.0, true, nil
			}
			// 非ユニークインデックス: foundRecords=RecPerKey のレンジスキャンとして扱う
			idxStats, ok := candidate.stats.IdxStats[idxMeta.Name]
			if ok {
				readTime := calcReadTimeForSecondaryIndex(idxStats.RecPerKey, pageReadCost)
				rangeCost := readTime + idxStats.RecPerKey*RowEvaluateCost + 0.01
				return rangeCost, idxStats.RecPerKey, true, nil
			}
		}
	}

	// レンジスキャン: PK
	if candidate.tblMeta.PKCount == 1 {
		if colMeta, exists := candidate.tblMeta.GetColByName(colName); exists && colMeta.Pos == 0 {
			if expr.Operator == ">" || expr.Operator == ">=" || expr.Operator == "<" || expr.Operator == "<=" {
				rhs := expr.Right.(*ast.RhsLiteral)
				primaryBTree := btree.NewBTree(candidate.table.MetaPageId)
				lowerKey, upperKey, leftIncl, rightIncl := buildRangeKeys(expr.Operator, rhs.Literal)
				foundRecords, err := primaryBTree.RecordsInRange(bp, lowerKey, upperKey, leftIncl, rightIncl)
				if err != nil {
					return 0, 0, false, err
				}
				readTime := calcReadTimeForClusteredIndex(float64(foundRecords), float64(candidate.stats.RecordCount), float64(candidate.stats.LeafPageCount), pageReadCost)
				// rangeCost = readTime + foundRecords × RowEvaluateCost + 0.01
				// (calcRangeScanCost は最終コストなので JOIN の prefix_cost 計算では使わない)
				rangeCost := readTime + float64(foundRecords)*RowEvaluateCost + 0.01
				return rangeCost, float64(foundRecords), true, nil
			}
		}
	}

	// レンジスキャン: UNIQUE INDEX
	if idxMeta, hasIdx := candidate.tblMeta.GetIndexByColName(colName); hasIdx {
		if expr.Operator == ">" || expr.Operator == ">=" || expr.Operator == "<" || expr.Operator == "<=" {
			rhs := expr.Right.(*ast.RhsLiteral)
			index, err := candidate.table.GetSecondaryIndexByName(idxMeta.Name)
			if err != nil {
				return 0, 0, false, err
			}
			indexBTree := btree.NewBTree(index.MetaPageId)
			lowerKey, upperKey, leftIncl, rightIncl := buildRangeKeys(expr.Operator, rhs.Literal)
			foundRecords, err := indexBTree.RecordsInRange(bp, lowerKey, upperKey, leftIncl, rightIncl)
			if err != nil {
				return 0, 0, false, err
			}
			// 非 index-only scan ではクラスタ化インデックスの pageReadCost を使う
			readTime := calcReadTimeForSecondaryIndex(float64(foundRecords), pageReadCost)
			rangeCost := readTime + float64(foundRecords)*RowEvaluateCost + 0.01
			return rangeCost, float64(foundRecords), true, nil
		}
	}

	return 0, 0, false, nil
}

// calcFiltered はインデックスで絞り込めない WHERE 条件の通過率を算出する
//
// PK/UNIQUE INDEX の等値・レンジ条件は fanout に既に反映されるので除外し、
// インデックスなしの条件のみ通過率として返す
func calcFiltered(expr *ast.BinaryExpr, candidate joinCandidate) float64 {
	if expr == nil {
		return 1.0
	}

	// AND: 左右の通過率を掛け合わせる
	if expr.Operator == "AND" {
		if lhsExpr, ok := expr.Left.(*ast.LhsExpr); ok {
			if rhsExpr, ok := expr.Right.(*ast.RhsExpr); ok {
				return calcFiltered(lhsExpr.Expr, candidate) * calcFiltered(rhsExpr.Expr, candidate)
			}
		}
	}

	// リーフ: col op literal
	lhs, lOk := expr.Left.(*ast.LhsColumn)
	if !lOk {
		return 1.0
	}
	colName := lhs.Column.ColName

	// PK/UNIQUE INDEX がある条件は fanout に既に反映されるため filtered = 1.0
	if candidate.tblMeta.PKCount == 1 {
		if colMeta, exists := candidate.tblMeta.GetColByName(colName); exists && colMeta.Pos == 0 {
			return 1.0
		}
	}
	if _, hasIdx := candidate.tblMeta.GetIndexByColName(colName); hasIdx {
		return 1.0
	}

	// インデックスなしの条件: 通過率を推定
	colStats, ok := candidate.stats.ColStats[colName]
	if !ok || colStats.UniqueValues == 0 {
		// 統計情報がない場合は MySQL のデフォルト (COND_FILTER_EQUALITY = 0.1) を使う
		return 0.1
	}

	switch expr.Operator {
	case "=":
		return 1.0 / float64(colStats.UniqueValues)
	case "!=":
		return float64(colStats.UniqueValues-1) / float64(colStats.UniqueValues)
	default:
		// レンジ演算子はデフォルト 1/3 (defaultRangeSelectivity 相当)
		return 1.0 / 3.0
	}
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
