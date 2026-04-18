package planner

import (
	"bytes"
	"fmt"
	"minesql/internal/ast"
	"minesql/internal/executor"
	"minesql/internal/storage/access"
	"minesql/internal/storage/handler"
)

func PlanSelect(trxId handler.TrxId, stmt *ast.SelectStmt) (executor.Executor, error) {
	if len(stmt.Joins) > 0 {
		return planSelectJoin(trxId, stmt)
	}
	return planSelectSingle(trxId, stmt)
}

// planSelectSingle は単一テーブルの SELECT を計画する (従来の処理)
func planSelectSingle(trxId handler.TrxId, stmt *ast.SelectStmt) (executor.Executor, error) {
	hdl := handler.Get()

	tblMeta, ok := hdl.Catalog.GetTableMetaByName(stmt.From.TableName)
	if !ok {
		return nil, fmt.Errorf("table %s not found", stmt.From.TableName)
	}

	rv := hdl.CreateReadView(trxId)
	vr := access.NewVersionReader(hdl.UndoLog())
	search := NewSearch(rv, vr, tblMeta, stmt.Where, hdl.BufferPool)
	iterator, err := search.Build()
	if err != nil {
		return nil, err
	}

	var colPos []uint16
	for _, colMeta := range tblMeta.Cols {
		colPos = append(colPos, colMeta.Pos)
	}
	return executor.NewProject(iterator, colPos), nil
}

// planSelectJoin は JOIN を含む SELECT を計画する
func planSelectJoin(trxId handler.TrxId, stmt *ast.SelectStmt) (executor.Executor, error) {
	hdl := handler.Get()
	rv := hdl.CreateReadView(trxId)
	vr := access.NewVersionReader(hdl.UndoLog())

	// 1. 参加テーブルのメタデータ・統計情報・テーブルオブジェクトを収集
	tableNames := collectTableNames(stmt)
	candidates, err := buildJoinCandidates(hdl, tableNames)
	if err != nil {
		return nil, err
	}

	// 2. ON 条件から joinPredicate を抽出
	predicates, err := extractJoinPredicates(stmt.Joins)
	if err != nil {
		return nil, err
	}

	// 3. 貪欲法で結合順序を決定
	ordered, err := optimizeJoinOrder(hdl.BufferPool, candidates, predicates)
	if err != nil {
		return nil, err
	}

	// 4. 結合順序に従って Executor ツリーを構築
	orderedMetas := make([]*handler.TableMetadata, len(ordered))
	for i, c := range ordered {
		orderedMetas[i] = c.tblMeta
	}
	joinedColumns := resolveJoinedColumns(orderedMetas)

	// 駆動表の TableScan
	var exec executor.Executor = executor.NewTableScan(executor.TableScanParams{
		ReadView:       rv,
		VersionReader:  vr,
		Table:          ordered[0].table,
		SearchMode:     access.RecordSearchModeStart{},
		WhileCondition: func(record executor.Record) bool { return true },
	})

	// 2 番目以降のテーブルを NestedLoopJoin で結合
	leftColCount := int(ordered[0].tblMeta.NCols)
	for i := 1; i < len(ordered); i++ {
		rightCandidate := ordered[i]
		pred := findPredicateForTable(predicates, rightCandidate.tblMeta.Name, ordered[:i])

		buildRight, err := buildRightExecFunc(rv, vr, rightCandidate, pred, joinedColumns)
		if err != nil {
			return nil, err
		}

		exec = executor.NewNestedLoopJoin(exec, buildRight)
		leftColCount += int(rightCandidate.tblMeta.NCols)
	}

	// 5. WHERE 句があれば Filter を重ねる (結合レコード全体のカラム位置で解決)
	if stmt.Where != nil {
		condFunc, err := buildJoinedConditionFunc(*stmt.Where.Condition, joinedColumns)
		if err != nil {
			return nil, err
		}
		exec = executor.NewFilter(exec, condFunc)
	}

	// 6. Project (SELECT * → 全カラム)
	colPos := make([]uint16, leftColCount)
	for i := range colPos {
		colPos[i] = uint16(i)
	}
	return executor.NewProject(exec, colPos), nil
}

// collectTableNames は SELECT 文から参加テーブル名を収集する
func collectTableNames(stmt *ast.SelectStmt) []string {
	names := []string{stmt.From.TableName}
	for _, join := range stmt.Joins {
		names = append(names, join.Table.TableName)
	}
	return names
}

// buildJoinCandidates は各テーブルの joinCandidate を構築する
func buildJoinCandidates(hdl *handler.Handler, tableNames []string) ([]joinCandidate, error) {
	candidates := make([]joinCandidate, 0, len(tableNames))
	for _, name := range tableNames {
		tblMeta, ok := hdl.Catalog.GetTableMetaByName(name)
		if !ok {
			return nil, fmt.Errorf("table %s not found", name)
		}
		stats, err := hdl.AnalyzeTable(tblMeta)
		if err != nil {
			return nil, err
		}
		tbl, err := hdl.GetTable(name)
		if err != nil {
			return nil, err
		}
		candidates = append(candidates, joinCandidate{tblMeta: tblMeta, stats: stats, table: tbl})
	}
	return candidates, nil
}

// extractJoinPredicates は JoinClause の ON 条件から joinPredicate を抽出する
func extractJoinPredicates(joins []*ast.JoinClause) ([]joinPredicate, error) {
	var predicates []joinPredicate
	for _, join := range joins {
		preds, err := extractPredicatesFromExpr(join.Condition)
		if err != nil {
			return nil, err
		}
		predicates = append(predicates, preds...)
	}
	return predicates, nil
}

// extractPredicatesFromExpr は BinaryExpr から joinPredicate を再帰的に抽出する
//
// AND で複数条件が結合されている場合も対応する
func extractPredicatesFromExpr(expr *ast.BinaryExpr) ([]joinPredicate, error) {
	if expr == nil {
		return nil, nil
	}

	// LhsColumn = RhsColumn の場合 → joinPredicate
	if lhs, ok := expr.Left.(*ast.LhsColumn); ok {
		if rhs, ok := expr.Right.(*ast.RhsColumn); ok {
			return []joinPredicate{{
				leftTable:  lhs.Column.TableName,
				leftCol:    lhs.Column.ColName,
				rightTable: rhs.Column.TableName,
				rightCol:   rhs.Column.ColName,
			}}, nil
		}
	}

	// AND の場合: 左右を再帰
	if lhsExpr, ok := expr.Left.(*ast.LhsExpr); ok {
		if rhsExpr, ok := expr.Right.(*ast.RhsExpr); ok {
			if expr.Operator == "AND" {
				left, err := extractPredicatesFromExpr(lhsExpr.Expr)
				if err != nil {
					return nil, err
				}
				right, err := extractPredicatesFromExpr(rhsExpr.Expr)
				if err != nil {
					return nil, err
				}
				return append(left, right...), nil
			}
		}
	}

	return nil, fmt.Errorf("unsupported ON condition structure")
}

// findPredicateForTable は ordered テーブル群の中から候補テーブルとの結合条件を探す
func findPredicateForTable(predicates []joinPredicate, tableName string, orderedBefore []joinCandidate) *joinPredicate {
	resultTableNames := make(map[string]struct{}, len(orderedBefore))
	for _, c := range orderedBefore {
		resultTableNames[c.tblMeta.Name] = struct{}{}
	}
	return findPredicate(predicates, tableName, resultTableNames)
}

// buildRightExecFunc は内部表の Executor ファクトリ関数を構築する
func buildRightExecFunc(
	rv *access.ReadView,
	vr *access.VersionReader,
	candidate joinCandidate,
	pred *joinPredicate,
	columns []joinedColumn,
) (func(executor.Record) (executor.Executor, error), error) {
	if pred == nil {
		return nil, fmt.Errorf("no join predicate for table %s", candidate.tblMeta.Name)
	}

	// 結合カラム: 左側 (結合レコード内の位置) と右側 (右テーブル内の位置)
	joinCol := resolveJoinCol(pred, candidate.tblMeta.Name)
	otherCol := resolveJoinCol(pred, pred.leftTable)
	if pred.leftTable == candidate.tblMeta.Name {
		otherCol = resolveJoinCol(pred, pred.rightTable)
	}

	// 左側の結合カラム位置 (結合レコード全体)
	otherTable := pred.leftTable
	if pred.leftTable == candidate.tblMeta.Name {
		otherTable = pred.rightTable
	}
	leftJoinColPos, err := findColumnPos(columns, otherTable, otherCol)
	if err != nil {
		return nil, err
	}

	// 右側の結合カラム位置 (右テーブル単体)
	rightColMeta, ok := candidate.tblMeta.GetColByName(joinCol)
	if !ok {
		return nil, fmt.Errorf("column %s not found in table %s", joinCol, candidate.tblMeta.Name)
	}
	rightJoinColPos := int(rightColMeta.Pos)

	// PK eq_ref
	if candidate.tblMeta.PKCount == 1 && rightJoinColPos == 0 {
		return func(leftRecord executor.Record) (executor.Executor, error) {
			key := leftRecord[leftJoinColPos]
			return executor.NewTableScan(executor.TableScanParams{
				ReadView:      rv,
				VersionReader: vr,
				Table:         candidate.table,
				SearchMode:    access.RecordSearchModeKey{Key: [][]byte{key}},
				WhileCondition: func(r executor.Record) bool {
					return bytes.Equal(r[0], key)
				},
			}), nil
		}, nil
	}

	// UNIQUE INDEX eq_ref
	if idxMeta, hasIdx := candidate.tblMeta.GetIndexByColName(joinCol); hasIdx {
		index, err := candidate.table.GetUniqueIndexByName(idxMeta.Name)
		if err != nil {
			return nil, err
		}
		return func(leftRecord executor.Record) (executor.Executor, error) {
			key := leftRecord[leftJoinColPos]
			// IndexScan の条件関数が受け取る Record はセカンダリキー値のみの 1 要素なので position 0 を使う
			cond := func(r executor.Record) bool {
				return bytes.Equal(r[0], key)
			}
			return executor.NewIndexScan(
				candidate.table,
				index,
				access.RecordSearchModeKey{Key: [][]byte{key}},
				cond,
			), nil
		}, nil
	}

	// フルスキャン
	return func(leftRecord executor.Record) (executor.Executor, error) {
		key := leftRecord[leftJoinColPos]
		return executor.NewFilter(
			executor.NewTableScan(executor.TableScanParams{
				ReadView:       rv,
				VersionReader:  vr,
				Table:          candidate.table,
				SearchMode:     access.RecordSearchModeStart{},
				WhileCondition: func(record executor.Record) bool { return true },
			}),
			func(rightRecord executor.Record) bool {
				return bytes.Equal(rightRecord[rightJoinColPos], key)
			},
		), nil
	}, nil
}
