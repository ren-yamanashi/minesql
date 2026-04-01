package planner

import (
	"errors"
	"fmt"
	"minesql/internal/ast"
	"minesql/internal/executor"
	"minesql/internal/storage/handler"
	"strconv"
)

// Search は WHERE 句に基づいてレコードを検索する Executor を構築する
type Search struct {
	tblMeta *handler.TableMetadata
	where   *ast.WhereClause
}

func NewSearch(tblMeta *handler.TableMetadata, where *ast.WhereClause) *Search {
	return &Search{
		tblMeta: tblMeta,
		where:   where,
	}
}

func (sp *Search) Build() (executor.Executor, error) {
	rawTbl, err := sp.tblMeta.GetTable()
	if err != nil {
		return nil, err
	}
	tbl := handler.NewTableHandler(rawTbl)

	// WHERE 句が設定されていない場合フルテーブルスキャンを実行
	if sp.where == nil || !sp.where.IsSet {
		return executor.NewTableScan(
			tbl,
			handler.SearchModeStart{},
			func(record executor.Record) bool {
				return true // フルテーブルスキャンなので常に true を返す
			},
		), nil
	}

	// WHERE 句が設定されている場合
	switch expr := sp.where.Condition.(type) {
	case *ast.BinaryExpr:
		return sp.planForBinaryExpr(tbl, *expr)
	default:
		return nil, errors.New("unsupported WHERE condition type")
	}
}

// leafCondition は複合条件中の単一リーフ条件 (col op literal) を表す
type leafCondition struct {
	colName  string
	operator string
	literal  ast.Literal
}

// planForBinaryExpr は二項演算式を解析して適切な検索用の Executor を構築する
//
// 式の構造に応じて以下のように分岐する:
//   - 単一条件 (col op literal): chooseBestPlan でテーブルスキャン / PK / インデックスを比較
//   - 純粋な AND 条件: extractANDLeaves でリーフを抽出 → chooseBestPlan
//   - OR を含む条件: planForORCondition で Union 最適化を試みる
func (s *Search) planForBinaryExpr(tbl *handler.TableHandler, expr ast.BinaryExpr) (executor.Executor, error) {
	switch lhs := expr.Left.(type) {

	// 単一条件: LhsColumn op RhsLiteral (例: WHERE col = 5)
	case *ast.LhsColumn:
		colName := lhs.Column.ColName
		switch rhs := expr.Right.(type) {
		case *ast.RhsLiteral:
			colMeta, ok := s.tblMeta.GetColByName(colName)
			if !ok {
				return nil, errors.New("column " + colName + " does not exist in table " + s.tblMeta.Name)
			}
			cond, err := s.operatorToCondition(expr.Operator, int(colMeta.Pos), rhs.Literal.ToString())
			if err != nil {
				return nil, err
			}
			leaves := []leafCondition{{colName: colName, operator: expr.Operator, literal: rhs.Literal}}
			return s.chooseBestPlan(tbl, leaves, cond)
		default:
			// col1 = col2 のようなカラム同士の比較は未サポート
			return nil, errors.New("when LHS is a column, RHS must be a literal")
		}

	// 複合条件: LhsExpr AND/OR RhsExpr (例: WHERE col1 = 5 AND col2 > 10)
	case *ast.LhsExpr:
		cond, err := s.buildConditionFunc(expr)
		if err != nil {
			return nil, err
		}

		// AND ツリーからリーフ条件を抽出 (OR を含む場合は nil が返る)
		leaves := extractANDLeaves(expr)
		if leaves != nil {
			return s.chooseBestPlan(tbl, leaves, cond)
		}

		// AND で分解できない(=OR を含む条件) → Union 最適化を試みる
		return s.planForORCondition(tbl, expr, cond)

	default:
		return nil, errors.New("unsupported LHS type in binary expression")
	}
}

// =================================================
// プラン選択
// =================================================

// chooseBestPlan は AND 条件に対して、以下の 3 種類のプランからコスト最安を選択する
//
//  1. テーブルスキャン + Filter (全条件をフィルタで適用)
//  2. PK スキャン (+ Filter で残条件を適用)
//  3. セカンダリインデックススキャン (+ Filter で残条件を適用)
func (s *Search) chooseBestPlan(tbl *handler.TableHandler, leaves []leafCondition, cond func(executor.Record) bool) (executor.Executor, error) {
	tableScanPlan := executor.NewFilter(
		executor.NewTableScan(
			tbl,
			handler.SearchModeStart{},
			func(record executor.Record) bool { return true },
		),
		cond,
	)

	// 統計情報を取得
	eng := handler.Get()
	stats, err := eng.AnalyzeTable(s.tblMeta)
	if err != nil {
		return nil, err
	}

	// テーブルスキャン + Filter プランのコストを見積もる
	// 複数の AND 条件がある場合、各条件の選択コスト関数を連鎖的に適用して
	// フィルタ後の R(s) や V(s,F) を推定する (Executor ノード数とは無関係)
	tableScanCost := calcTableScanCost(stats)
	for _, leaf := range leaves {
		tableScanCost = s.applySelectionCost(tableScanCost, leaf.colName, leaf.operator, leaf.literal, stats)
	}

	// 各リーフ条件について、セカンダリインデックス / PK スキャンのコストを算出し最安を記録
	var bestIdxLeaf *leafCondition
	var bestIdxCost ScanCost
	var bestPKLeaf *leafCondition
	var bestPKCost ScanCost
	for i := range leaves {
		leaf := &leaves[i]

		// セカンダリインデックスのコストを算出
		idxMeta, hasIndex := s.tblMeta.GetIndexByColName(leaf.colName)
		if hasIndex {
			idxStats, ok := stats.IdxStats[idxMeta.Name]
			if ok {
				cost := s.calcIndexPlanCost(stats, leaf.colName, leaf.operator, leaf.literal, idxStats)
				if bestIdxLeaf == nil || cost.TotalCost() < bestIdxCost.TotalCost() {
					bestIdxLeaf = leaf
					bestIdxCost = cost
				}
			}
		}

		// PK スキャンのコストを算出
		if s.isPKLeadingColumn(leaf.colName) {
			cost := s.calcPKPlanCost(stats, leaf.colName, leaf.operator, leaf.literal)
			if bestPKLeaf == nil || cost.TotalCost() < bestPKCost.TotalCost() {
				bestPKLeaf = leaf
				bestPKCost = cost
			}
		}
	}

	// 3-way 比較: テーブルスキャン vs インデックス vs PK
	minCost := tableScanCost.TotalCost()
	bestPlan := "TableScan"
	if bestIdxLeaf != nil && bestIdxCost.TotalCost() < minCost {
		minCost = bestIdxCost.TotalCost()
		bestPlan = "Index"
	}
	if bestPKLeaf != nil && bestPKCost.TotalCost() < minCost {
		bestPlan = "PK"
	}

	switch bestPlan {
	case "Index":
		idxMeta, _ := s.tblMeta.GetIndexByColName(bestIdxLeaf.colName)
		return s.buildIndexPlan(tbl, *bestIdxLeaf, idxMeta, cond, len(leaves) > 1)
	case "PK":
		return s.buildPKScanPlan(tbl, *bestPKLeaf, cond, len(leaves) > 1), nil
	default:
		return tableScanPlan, nil
	}
}

// buildIndexPlan はインデックスを使った Executor を構築する
//
// needsFilter が true の場合、IndexScan の上に Filter を重ねる (複合条件時)
func (s *Search) buildIndexPlan(tbl *handler.TableHandler, leaf leafCondition, idxMeta *handler.IndexMetadata, cond func(executor.Record) bool, needsFilter bool) (executor.Executor, error) {
	index, err := tbl.GetUniqueIndexByName(idxMeta.Name)
	if err != nil {
		return nil, err
	}
	indexCond, err := s.operatorToCondition(leaf.operator, 0, leaf.literal.ToString())
	if err != nil {
		return nil, err
	}

	scan := executor.NewIndexScan(
		tbl,
		index,
		handler.SearchModeKey{Key: [][]byte{leaf.literal.ToBytes()}},
		indexCond,
	)

	if needsFilter {
		return executor.NewFilter(scan, cond), nil
	}
	return scan, nil
}

// buildPKScanPlan は PK カラムの条件を使った TableScan を構築する
//
// needsFilter が true の場合、TableScan の上に Filter を重ねる (複合条件時や > 演算子時)
func (s *Search) buildPKScanPlan(tbl *handler.TableHandler, leaf leafCondition, cond func(executor.Record) bool, needsFilter bool) executor.Executor {
	colMeta, _ := s.tblMeta.GetColByName(leaf.colName)
	pos := int(colMeta.Pos)
	value := leaf.literal.ToString()

	var searchMode handler.SearchMode
	var whileCond func(executor.Record) bool
	filterRequired := needsFilter

	// 演算子ごとに B+Tree のアクセスパターンを決定する
	// - =, >=, >: キー位置にシークして末尾方向へ走査
	// - <, <=: 先頭から走査して条件外で停止 (whileCondition)
	switch leaf.operator {
	case "=":
		searchMode = handler.SearchModeKey{Key: [][]byte{leaf.literal.ToBytes()}}
		whileCond = func(r executor.Record) bool { return string(r[pos]) == value }
	case ">=":
		searchMode = handler.SearchModeKey{Key: [][]byte{leaf.literal.ToBytes()}}
		whileCond = func(r executor.Record) bool { return true }
	case ">":
		searchMode = handler.SearchModeKey{Key: [][]byte{leaf.literal.ToBytes()}}
		whileCond = func(r executor.Record) bool { return true }
		filterRequired = true // 開始位置の等値レコードを除外するため Filter が必要
	case "<":
		searchMode = handler.SearchModeStart{}
		whileCond = func(r executor.Record) bool { return string(r[pos]) < value }
	case "<=":
		searchMode = handler.SearchModeStart{}
		whileCond = func(r executor.Record) bool { return string(r[pos]) <= value }
	default:
		// != などの場合はフルテーブルスキャン + Filter
		searchMode = handler.SearchModeStart{}
		whileCond = func(r executor.Record) bool { return true }
		filterRequired = true
	}

	scan := executor.NewTableScan(tbl, searchMode, whileCond)
	if filterRequired {
		return executor.NewFilter(scan, cond)
	}
	return scan
}

// =================================================
// OR 条件の最適化
// =================================================

// planForORCondition は OR 条件に対して Union による最適化を試みる
//
// 各 OR ブランチが PK またはセカンダリインデックスを利用できる場合、各ブランチを個別にスキャンし Union で結合する
//
// 最適化できない場合はテーブルスキャン + Filter にフォールバックする
func (s *Search) planForORCondition(tbl *handler.TableHandler, expr ast.BinaryExpr, cond func(executor.Record) bool) (executor.Executor, error) {
	tableScanPlan := executor.NewFilter(
		executor.NewTableScan(
			tbl,
			handler.SearchModeStart{},
			func(record executor.Record) bool { return true },
		),
		cond,
	)

	branches := extractORBranches(expr)
	if branches == nil {
		return tableScanPlan, nil
	}

	// 統計情報を取得
	eng := handler.Get()
	stats, err := eng.AnalyzeTable(s.tblMeta)
	if err != nil {
		return nil, err
	}

	// 各ブランチを PK/インデックスで個別にプラン構築する
	// 1 つでも PK/インデックスが使えないブランチがあれば Union は不可
	var executors []executor.Executor
	var totalCost float64
	for _, branch := range branches {
		exec, cost, ok := s.planORBranch(tbl, branch, stats)
		if !ok {
			return tableScanPlan, nil
		}
		executors = append(executors, exec)
		totalCost += cost
	}

	// Union の合計コスト < テーブルスキャンのコストなら Union を採用
	tableCost := calcTableScanCost(stats)
	if totalCost < tableCost.TotalCost() {
		return executor.NewUnion(executors), nil
	}

	return tableScanPlan, nil
}

// orBranch は OR 条件の各ブランチを表す
//
// 単一条件 (col op literal) または複合 AND 条件 (複数の leafCondition) を保持する
type orBranch struct {
	leaves []leafCondition // AND で結合されたリーフ条件群
	expr   ast.BinaryExpr  // ブランチの元の AST (条件関数の構築に使用)
}

// planORBranch は OR の各ブランチに対して PK またはインデックスを使ったプランを構築する
//
// ブランチが複合 AND 条件の場合、最もコストの安い PK/インデックスを選択し、
// 残りの条件は Filter で適用する
//
// PK/インデックスが利用できない場合は ok=false を返す
func (s *Search) planORBranch(tbl *handler.TableHandler, branch orBranch, stats *handler.TableStatistics) (executor.Executor, float64, bool) {
	// ブランチ全体の条件関数を構築 (複合 AND 条件時に Filter で使用)
	branchCond, err := s.buildConditionFunc(branch.expr)
	if err != nil {
		return nil, 0, false
	}

	// 複合条件の場合、最適な 1 条件で PK/インデックスを使い、残条件は Filter で適用
	needsFilter := len(branch.leaves) > 1

	// 各リーフ条件について PK/インデックスのコストを算出し、最安を選択
	var bestLeaf *leafCondition
	var bestCost float64
	var bestPlan string // "PK" or "Index"

	for i := range branch.leaves {
		leaf := &branch.leaves[i]

		// != は PK/インデックスで最適化しても効果が薄いため対象外
		if leaf.operator == "!=" {
			continue
		}

		// PK スキャンのコストを算出
		if s.isPKLeadingColumn(leaf.colName) {
			cost := s.calcPKPlanCost(stats, leaf.colName, leaf.operator, leaf.literal)
			if bestLeaf == nil || cost.TotalCost() < bestCost {
				bestLeaf = leaf
				bestCost = cost.TotalCost()
				bestPlan = "PK"
			}
		}

		// セカンダリインデックススキャンのコストを算出
		idxMeta, hasIndex := s.tblMeta.GetIndexByColName(leaf.colName)
		if hasIndex {
			idxStats, ok := stats.IdxStats[idxMeta.Name]
			if ok {
				cost := s.calcIndexPlanCost(stats, leaf.colName, leaf.operator, leaf.literal, idxStats)
				if bestLeaf == nil || cost.TotalCost() < bestCost {
					bestLeaf = leaf
					bestCost = cost.TotalCost()
					bestPlan = "Index"
				}
			}
		}
	}

	if bestLeaf == nil {
		return nil, 0, false
	}

	switch bestPlan {
	case "PK":
		exec := s.buildPKScanPlan(tbl, *bestLeaf, branchCond, needsFilter)
		return exec, bestCost, true
	case "Index":
		idxMeta, _ := s.tblMeta.GetIndexByColName(bestLeaf.colName)
		exec, err := s.buildIndexPlan(tbl, *bestLeaf, idxMeta, branchCond, needsFilter)
		if err != nil {
			return nil, 0, false
		}
		return exec, bestCost, true
	}

	return nil, 0, false
}

// =================================================
// リーフ条件の抽出
// =================================================

// extractANDLeaves は純粋な AND ツリーからリーフ条件を抽出する
//
// OR が含まれている場合は nil を返す
func extractANDLeaves(expr ast.BinaryExpr) []leafCondition {
	// リーフ: LhsColumn op RhsLiteral
	if lhs, ok := expr.Left.(*ast.LhsColumn); ok {
		if rhs, ok := expr.Right.(*ast.RhsLiteral); ok {
			return []leafCondition{{
				colName:  lhs.Column.ColName,
				operator: expr.Operator,
				literal:  rhs.Literal,
			}}
		}
		return nil
	}

	// ブランチ: LhsExpr AND/OR RhsExpr
	lhsExpr, lhsOk := expr.Left.(*ast.LhsExpr)
	rhsExpr, rhsOk := expr.Right.(*ast.RhsExpr)
	if !lhsOk || !rhsOk {
		return nil
	}

	// OR が含まれていたら最適化不可
	if expr.Operator != "AND" {
		return nil
	}

	leftLeaves := extractANDLeaves(*lhsExpr.Expr.(*ast.BinaryExpr))
	if leftLeaves == nil {
		return nil
	}
	rightLeaves := extractANDLeaves(*rhsExpr.Expr.(*ast.BinaryExpr))
	if rightLeaves == nil {
		return nil
	}

	return append(leftLeaves, rightLeaves...)
}

// extractORBranches は OR ツリーから各ブランチを抽出する
//
// 各ブランチは単一条件 (col op literal) または複合 AND 条件を保持できる
// AND サブツリーは extractANDLeaves でリーフ条件に分解する
// 分解できないブランチがある場合は nil を返す
func extractORBranches(expr ast.BinaryExpr) []orBranch {
	// リーフ: LhsColumn op RhsLiteral → 単一条件のブランチ
	if lhs, ok := expr.Left.(*ast.LhsColumn); ok {
		if rhs, ok := expr.Right.(*ast.RhsLiteral); ok {
			leaf := leafCondition{
				colName:  lhs.Column.ColName,
				operator: expr.Operator,
				literal:  rhs.Literal,
			}
			return []orBranch{{leaves: []leafCondition{leaf}, expr: expr}}
		}
		return nil
	}

	// ブランチ: LhsExpr op RhsExpr
	lhsExpr, lhsOk := expr.Left.(*ast.LhsExpr)
	rhsExpr, rhsOk := expr.Right.(*ast.RhsExpr)
	if !lhsOk || !rhsOk {
		return nil
	}

	if expr.Operator == "OR" {
		// OR ノード: 左右を再帰して連結
		leftBranches := extractORBranches(*lhsExpr.Expr.(*ast.BinaryExpr))
		if leftBranches == nil {
			return nil
		}
		rightBranches := extractORBranches(*rhsExpr.Expr.(*ast.BinaryExpr))
		if rightBranches == nil {
			return nil
		}
		return append(leftBranches, rightBranches...)
	}

	// AND ノード (またはその他): サブツリー全体を 1 つのブランチとして扱う
	leaves := extractANDLeaves(expr)
	if leaves == nil {
		return nil
	}
	return []orBranch{{leaves: leaves, expr: expr}}
}

// =================================================
// コスト計算
// =================================================

// calcPKPlanCost は PK スキャンのコストを算出する
func (s *Search) calcPKPlanCost(stats *handler.TableStatistics, colName string, operator string, literal ast.Literal) ScanCost {
	inner := calcTableScanCost(stats)
	switch operator {
	case "=":
		return calcPKSelectEqualCost(inner, colName, stats.TreeHeight)
	case ">", ">=":
		sel := s.calcSelectivity(colName, operator, literal, stats)
		return calcPKSelectRangeGTCost(inner, colName, sel, stats.TreeHeight)
	case "<", "<=":
		sel := s.calcSelectivity(colName, operator, literal, stats)
		return calcPKSelectRangeLTCost(inner, colName, sel)
	default:
		// != やサポートされていない演算子は PK 最適化の対象外 (高コストを返す)
		return ScanCost{DiskAccesses: float64(stats.LeafPageCount) * 2}
	}
}

// applySelectionCost はテーブルスキャンコストに WHERE 条件の選択コストを適用する
func (s *Search) applySelectionCost(baseCost ScanCost, colName string, operator string, literal ast.Literal, stats *handler.TableStatistics) ScanCost {
	switch operator {
	case "=":
		return calcSelectEqualCost(baseCost, colName)
	case "!=":
		return calcSelectNotEqualCost(baseCost, colName)
	case ">", ">=", "<", "<=":
		sel := s.calcSelectivity(colName, operator, literal, stats)
		return calcSelectRangeCost(baseCost, colName, sel)
	default:
		return baseCost
	}
}

// calcIndexPlanCost はインデックス+テーブルプランのコストを算出する
func (s *Search) calcIndexPlanCost(stats *handler.TableStatistics, colName string, operator string, literal ast.Literal, idxStats handler.IndexStatistics) ScanCost {
	switch operator {
	case "=":
		return calcIndexTableEqualCost(stats, colName, idxStats.Height, stats.TreeHeight)
	case "!=":
		return calcIndexTableNotEqualCost(stats, colName, idxStats.Height, idxStats.LeafPageCount, stats.TreeHeight)
	case ">", ">=", "<", "<=":
		sel := s.calcSelectivity(colName, operator, literal, stats)
		return calcIndexTableRangeCost(stats, colName, idxStats.Height, idxStats.LeafPageCount, sel, stats.TreeHeight)
	default:
		// サポートされていない演算子の場合は高コストを返してテーブルスキャンを選ばせる
		return ScanCost{DiskAccesses: float64(stats.LeafPageCount) * 2}
	}
}

// calcSelectivity は範囲比較の選択率を算出する
func (s *Search) calcSelectivity(colName string, operator string, literal ast.Literal, stats *handler.TableStatistics) float64 {
	colStats, ok := stats.ColStats[colName]
	if !ok {
		return defaultRangeSelectivity
	}

	// min/max が []byte なので float64 に変換を試みる
	c, err := strconv.ParseFloat(literal.ToString(), 64)
	if err != nil {
		// 数値に変換できない場合はデフォルト値
		return defaultRangeSelectivity
	}
	minVal, err := strconv.ParseFloat(string(colStats.MinValue), 64)
	if err != nil {
		return defaultRangeSelectivity
	}
	maxVal, err := strconv.ParseFloat(string(colStats.MaxValue), 64)
	if err != nil {
		return defaultRangeSelectivity
	}

	return calcRangeSelectivity(operator, c, minVal, maxVal)
}

// =================================================
// 条件関数の構築
// =================================================

// buildConditionFunc は式の木構造から単一の条件関数を再帰的に構築する
func (s *Search) buildConditionFunc(expr ast.BinaryExpr) (func(executor.Record) bool, error) {
	switch lhs := expr.Left.(type) {

	// リーフノード: col op literal のような単純な条件 (例: col1 = 5)
	case *ast.LhsColumn:
		colName := lhs.Column.ColName
		colMeta, ok := s.tblMeta.GetColByName(colName)
		if !ok {
			return nil, errors.New("column " + colName + " does not exist in table " + s.tblMeta.Name)
		}

		switch rhs := expr.Right.(type) {
		// 左辺がカラムで右辺がリテラルの場合 (例: col1 = 5)
		case *ast.RhsLiteral:
			return s.operatorToCondition(expr.Operator, int(colMeta.Pos), rhs.Literal.ToString())
		// 左辺がカラムの場合は右辺はリテラルでなければならない (`col1 = col2` のような条件は現状サポートしていない)
		default:
			return nil, errors.New("when LHS is a column, RHS must be a literal")
		}

	// ブランチノード: expr AND/OR expr (例: col1 = 5 AND col2 > 10 のような複合条件)
	case *ast.LhsExpr:
		// 左辺の式から条件関数を再帰的に構築
		leftCond, err := s.buildConditionFunc(*lhs.Expr.(*ast.BinaryExpr))
		if err != nil {
			return nil, err
		}

		switch rhs := expr.Right.(type) {
		// 右辺が式の場合、右辺の式から条件関数を再帰的に構築し、論理演算子 (AND/OR) に応じて条件関数を組み合わせる
		case *ast.RhsExpr:
			rightCond, err := s.buildConditionFunc(*rhs.Expr.(*ast.BinaryExpr))
			if err != nil {
				return nil, err
			}
			switch expr.Operator {
			case "AND":
				return func(r executor.Record) bool { return leftCond(r) && rightCond(r) }, nil
			case "OR":
				return func(r executor.Record) bool { return leftCond(r) || rightCond(r) }, nil
			default:
				return nil, fmt.Errorf("unsupported logical operator: %s", expr.Operator)
			}
		// 左辺が式の場合は右辺も式でなければならない
		default:
			return nil, errors.New("when LHS is an expression, RHS cannot be a literal")
		}

	default:
		panic("unsupported LHS type in buildConditionFunc")
	}
}

// operatorToCondition は二項演算子を条件関数に変換する
//
// 条件関数: レコードを受け取り、条件を満たすかどうか (bool) を返す関数
func (s *Search) operatorToCondition(operator string, pos int, value string) (func(executor.Record) bool, error) {
	switch operator {
	case "=":
		return func(record executor.Record) bool {
			return string(record[pos]) == value
		}, nil
	case "!=":
		return func(record executor.Record) bool {
			return string(record[pos]) != value
		}, nil
	case "<":
		return func(record executor.Record) bool {
			return string(record[pos]) < value
		}, nil
	case "<=":
		return func(record executor.Record) bool {
			return string(record[pos]) <= value
		}, nil
	case ">":
		return func(record executor.Record) bool {
			return string(record[pos]) > value
		}, nil
	case ">=":
		return func(record executor.Record) bool {
			return string(record[pos]) >= value
		}, nil
	default:
		return nil, fmt.Errorf("unsupported operator in WHERE clause: %s", operator)
	}
}

// =================================================
// その他
// =================================================

// isPKLeadingColumn は指定カラムがプライマリキーの先頭カラムかどうかを判定する
func (s *Search) isPKLeadingColumn(colName string) bool {
	if s.tblMeta.PKCount == 0 {
		return false
	}
	colMeta, ok := s.tblMeta.GetColByName(colName)
	return ok && colMeta.Pos == 0
}
