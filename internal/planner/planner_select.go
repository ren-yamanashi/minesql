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
	search.SetSelectColumns(stmt.Columns)
	iterator, err := search.Build()
	if err != nil {
		return nil, err
	}

	colPos, err := resolveSelectColumns(stmt.Columns, []*handler.TableMetadata{tblMeta})
	if err != nil {
		return nil, err
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
	// 全テーブルのメタデータ (非修飾カラム名が複数テーブルに存在するか判定するために使用)
	allMetas := make([]*handler.TableMetadata, len(candidates))
	for i, c := range candidates {
		allMetas[i] = c.tblMeta
	}
	ordered, err := optimizeJoinOrder(hdl.BufferPool, candidates, predicates, stmt.Where, allMetas)
	if err != nil {
		return nil, err
	}

	// 4. 結合順序に従って Executor ツリーを構築
	orderedMetas := make([]*handler.TableMetadata, len(ordered))
	for i, c := range ordered {
		orderedMetas[i] = c.tblMeta
	}
	joinedColumns := resolveJoinedColumns(orderedMetas)

	// 駆動表のアクセスパスを決定
	// WHERE 条件のうち駆動表のカラムのみに関係する条件を抽出し、Search で最適化する
	drivingTable := ordered[0]
	drivingWhere, remainingWhere := splitWhereForTable(stmt.Where, drivingTable.tblMeta, orderedMetas)
	search := NewSearch(rv, vr, drivingTable.tblMeta, drivingWhere, hdl.BufferPool)
	exec, err := search.Build()
	if err != nil {
		return nil, err
	}

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

	// 5. 駆動表に分離されなかった WHERE 条件があれば Filter を重ねる
	if remainingWhere != nil {
		condFunc, err := buildJoinedConditionFunc(*remainingWhere.Condition, joinedColumns)
		if err != nil {
			return nil, err
		}
		exec = executor.NewFilter(exec, condFunc)
	}

	// 6. Project
	colPos, err := resolveSelectColumnsForJoin(stmt.Columns, joinedColumns, leftColCount)
	if err != nil {
		return nil, err
	}
	return executor.NewProject(exec, colPos), nil
}

// splitWhereForTable は WHERE 条件を「指定テーブルのカラムのみの条件」と「残り」に分離する
//
// AND で結合された条件を分解し、指定テーブルのカラムのみで構成される部分を抽出する
// 抽出できない場合や WHERE が nil の場合は (nil, 元の WHERE) を返す
func splitWhereForTable(where *ast.WhereClause, tblMeta *handler.TableMetadata, allTables []*handler.TableMetadata) (forTable *ast.WhereClause, remaining *ast.WhereClause) {
	if where == nil {
		return nil, nil
	}

	tableExprs, otherExprs := splitExprForTable(where.Condition, tblMeta, allTables)

	var forTableWhere *ast.WhereClause
	if len(tableExprs) > 0 {
		combined := combineExprsWithAND(tableExprs)
		forTableWhere = &ast.WhereClause{Condition: combined}
	}

	var remainingWhere *ast.WhereClause
	if len(otherExprs) > 0 {
		combined := combineExprsWithAND(otherExprs)
		remainingWhere = &ast.WhereClause{Condition: combined}
	}

	return forTableWhere, remainingWhere
}

// splitExprForTable は BinaryExpr を再帰的に走査し、指定テーブルのカラムのみを参照する式を分離する
func splitExprForTable(expr *ast.BinaryExpr, tblMeta *handler.TableMetadata, allTables []*handler.TableMetadata) (forTable []*ast.BinaryExpr, other []*ast.BinaryExpr) {
	if expr == nil {
		return nil, nil
	}

	// AND の場合: 左右を再帰的に分離
	if expr.Operator == "AND" {
		if lhsExpr, ok := expr.Left.(*ast.LhsExpr); ok {
			if rhsExpr, ok := expr.Right.(*ast.RhsExpr); ok {
				lt, lo := splitExprForTable(lhsExpr.Expr, tblMeta, allTables)
				rt, ro := splitExprForTable(rhsExpr.Expr, tblMeta, allTables)
				return append(lt, rt...), append(lo, ro...)
			}
		}
	}

	// リーフ条件: テーブルに属するか判定
	if belongsToTable(expr, tblMeta, allTables) {
		return []*ast.BinaryExpr{expr}, nil
	}
	return nil, []*ast.BinaryExpr{expr}
}

// belongsToTable は式が指定テーブルのカラムのみを参照するか判定する
//
// allTables を渡した場合、非修飾名で同名カラムが複数テーブルにあるケースは
// 曖昧と判定して false を返す (分離しない)
func belongsToTable(expr *ast.BinaryExpr, tblMeta *handler.TableMetadata, allTables []*handler.TableMetadata) bool {
	lhs, ok := expr.Left.(*ast.LhsColumn)
	if !ok {
		return false
	}

	// 修飾名の場合: テーブル名が一致するか
	if lhs.Column.TableName != "" {
		return lhs.Column.TableName == tblMeta.Name
	}

	// 非修飾名の場合: そのカラムがテーブルに存在し、かつ他テーブルに同名カラムがないか
	_, exists := tblMeta.GetColByName(lhs.Column.ColName)
	if !exists {
		return false
	}
	for _, other := range allTables {
		if other.Name == tblMeta.Name {
			continue
		}
		if _, dup := other.GetColByName(lhs.Column.ColName); dup {
			return false // 同名カラムが他テーブルにもある → 曖昧なので分離しない
		}
	}
	return true
}

// combineExprsWithAND は複数の式を AND で結合する
func combineExprsWithAND(exprs []*ast.BinaryExpr) *ast.BinaryExpr {
	if len(exprs) == 1 {
		return exprs[0]
	}
	// 左から順に AND で結合
	result := exprs[0]
	for _, e := range exprs[1:] {
		result = ast.NewBinaryExpr("AND", ast.NewLhsExpr(result), ast.NewRhsExpr(e))
	}
	return result
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
		index, err := candidate.table.GetSecondaryIndexByName(idxMeta.Name)
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
