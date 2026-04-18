package planner

import (
	"errors"

	"minesql/internal/ast"
	"minesql/internal/executor"
	"minesql/internal/storage/access"
	"minesql/internal/storage/btree"
	"minesql/internal/storage/buffer"
	"minesql/internal/storage/encode"
	"minesql/internal/storage/handler"
)

// Search は WHERE 句に基づいてレコードを検索する Executor を構築する
type Search struct {
	readView      *access.ReadView
	versionReader *access.VersionReader
	tblMeta       *handler.TableMetadata
	where         *ast.WhereClause
	bufferPool    *buffer.BufferPool
}

func NewSearch(readView *access.ReadView, versionReader *access.VersionReader, tblMeta *handler.TableMetadata, where *ast.WhereClause, bp *buffer.BufferPool) *Search {
	return &Search{
		readView:      readView,
		versionReader: versionReader,
		tblMeta:       tblMeta,
		where:         where,
		bufferPool:    bp,
	}
}

func (sp *Search) Build() (executor.Executor, error) {
	tbl, err := handler.Get().GetTable(sp.tblMeta.Name)
	if err != nil {
		return nil, err
	}

	// WHERE 句が設定されていない場合フルテーブルスキャンを実行
	if sp.where == nil {
		return executor.NewTableScan(executor.TableScanParams{
			ReadView:       sp.readView,
			VersionReader:  sp.versionReader,
			Table:          tbl,
			SearchMode:     access.RecordSearchModeStart{},
			WhileCondition: func(record executor.Record) bool { return true },
		}), nil
	}

	// WHERE 句が設定されている場合
	return sp.planForBinaryExpr(tbl, *sp.where.Condition)
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
func (s *Search) planForBinaryExpr(tbl *access.Table, expr ast.BinaryExpr) (executor.Executor, error) {
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

// -------------------------------------------------
// プラン選択
// -------------------------------------------------

// chooseBestPlan は AND 条件に対して、以下の 3 種類のプランからコスト最安を選択する
//
//  1. テーブルスキャン + Filter (全条件をフィルタで適用)
//  2. PK スキャン (+ Filter で残条件を適用)
//  3. セカンダリインデックススキャン (+ Filter で残条件を適用)
func (s *Search) chooseBestPlan(tbl *access.Table, leaves []leafCondition, cond func(executor.Record) bool) (executor.Executor, error) {
	tableScanPlan := executor.NewFilter(
		executor.NewTableScan(executor.TableScanParams{
			ReadView:       s.readView,
			VersionReader:  s.versionReader,
			Table:          tbl,
			SearchMode:     access.RecordSearchModeStart{},
			WhileCondition: func(record executor.Record) bool { return true },
		}),
		cond,
	)

	// 統計情報を取得
	eng := handler.Get()
	stats, err := eng.AnalyzeTable(s.tblMeta)
	if err != nil {
		return nil, err
	}

	// フルスキャンのコスト
	primaryBTree := btree.NewBTree(tbl.MetaPageId)
	clusterPageReadCost, err := calcPageReadCost(s.bufferPool, primaryBTree)
	if err != nil {
		return nil, err
	}
	fullScanCost := calcFullScanCost(stats, clusterPageReadCost)

	// 各リーフ条件について PK/インデックスのコストを算出し最安を記録
	var bestLeaf *leafCondition
	var bestCost float64
	var bestPlan string // "PK", "Index", or ""
	uniqueFound := false

	for i := range leaves {
		leaf := &leaves[i]

		// != はレンジ分析の対象外 (フルスキャンにフォールバック)
		if leaf.operator == "!=" {
			continue
		}

		// ユニークスキャン判定: PK or UNIQUE INDEX の = 検索はコスト 1.0 で即確定
		if leaf.operator == "=" {
			if s.isPKLeadingColumn(leaf.colName) {
				bestLeaf = leaf
				bestCost = calcUniqueScanCost()
				bestPlan = "PK"
				uniqueFound = true
				break
			}
			if _, hasIndex := s.tblMeta.GetIndexByColName(leaf.colName); hasIndex {
				bestLeaf = leaf
				bestCost = calcUniqueScanCost()
				bestPlan = "Index"
				uniqueFound = true
				break
			}
		}

		// PK レンジスキャン
		if s.isPKLeadingColumn(leaf.colName) {
			lowerKey, upperKey, leftIncl, rightIncl := buildRangeKeys(leaf.operator, leaf.literal)
			foundRecords, err := primaryBTree.RecordsInRange(s.bufferPool, lowerKey, upperKey, leftIncl, rightIncl)
			if err != nil {
				return nil, err
			}
			readTime := calcReadTimeForClusteredIndex(
				1, float64(foundRecords), float64(stats.RecordCount), float64(stats.LeafPageCount), clusterPageReadCost,
			)
			cost := calcRangeScanCost(readTime, float64(foundRecords))
			if bestLeaf == nil || cost < bestCost {
				bestLeaf = leaf
				bestCost = cost
				bestPlan = "PK"
			}
		}

		// セカンダリインデックスのレンジスキャン
		idxMeta, hasIndex := s.tblMeta.GetIndexByColName(leaf.colName)
		if hasIndex {
			index, err := tbl.GetUniqueIndexByName(idxMeta.Name)
			if err != nil {
				return nil, err
			}
			indexBTree := btree.NewBTree(index.MetaPageId)
			lowerKey, upperKey, leftIncl, rightIncl := buildRangeKeys(leaf.operator, leaf.literal)
			foundRecords, err := indexBTree.RecordsInRange(s.bufferPool, lowerKey, upperKey, leftIncl, rightIncl)
			if err != nil {
				return nil, err
			}
			idxPageReadCost, err := calcPageReadCost(s.bufferPool, indexBTree)
			if err != nil {
				return nil, err
			}
			readTime := calcReadTimeForSecondaryIndex(1, float64(foundRecords), idxPageReadCost)
			cost := calcRangeScanCost(readTime, float64(foundRecords))
			if bestLeaf == nil || cost < bestCost {
				bestLeaf = leaf
				bestCost = cost
				bestPlan = "Index"
			}
		}
	}

	// フルスキャン vs 最安プラン
	if bestLeaf == nil || (!uniqueFound && fullScanCost <= bestCost) {
		return tableScanPlan, nil
	}

	switch bestPlan {
	case "Index":
		idxMeta, _ := s.tblMeta.GetIndexByColName(bestLeaf.colName)
		return s.buildIndexPlan(tbl, *bestLeaf, idxMeta, cond, len(leaves) > 1)
	case "PK":
		return s.buildPKScanPlan(tbl, *bestLeaf, cond, len(leaves) > 1), nil
	default:
		return tableScanPlan, nil
	}
}

// buildIndexPlan はインデックスを使った Executor を構築する
//
// needsFilter が true の場合、IndexScan の上に Filter を重ねる (複合条件時)
func (s *Search) buildIndexPlan(tbl *access.Table, leaf leafCondition, idxMeta *handler.IndexMetadata, cond func(executor.Record) bool, needsFilter bool) (executor.Executor, error) {
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
		access.RecordSearchModeKey{Key: [][]byte{leaf.literal.ToBytes()}},
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
func (s *Search) buildPKScanPlan(tbl *access.Table, leaf leafCondition, cond func(executor.Record) bool, needsFilter bool) executor.Executor {
	colMeta, _ := s.tblMeta.GetColByName(leaf.colName)
	pos := int(colMeta.Pos)
	value := leaf.literal.ToString()

	var searchMode access.RecordSearchMode
	var whileCond func(executor.Record) bool
	filterRequired := needsFilter

	// 演算子ごとに B+Tree のアクセスパターンを決定する
	// - =, >=, >: キー位置にシークして末尾方向へ走査
	// - <, <=: 先頭から走査して条件外で停止 (whileCondition)
	switch leaf.operator {
	case "=":
		searchMode = access.RecordSearchModeKey{Key: [][]byte{leaf.literal.ToBytes()}}
		whileCond = func(r executor.Record) bool { return string(r[pos]) == value }
	case ">=":
		searchMode = access.RecordSearchModeKey{Key: [][]byte{leaf.literal.ToBytes()}}
		whileCond = func(r executor.Record) bool { return true }
	case ">":
		searchMode = access.RecordSearchModeKey{Key: [][]byte{leaf.literal.ToBytes()}}
		whileCond = func(r executor.Record) bool { return true }
		filterRequired = true // 開始位置の等値レコードを除外するため Filter が必要
	case "<":
		searchMode = access.RecordSearchModeStart{}
		whileCond = func(r executor.Record) bool { return string(r[pos]) < value }
	case "<=":
		searchMode = access.RecordSearchModeStart{}
		whileCond = func(r executor.Record) bool { return string(r[pos]) <= value }
	default:
		// != などの場合はフルテーブルスキャン + Filter
		searchMode = access.RecordSearchModeStart{}
		whileCond = func(r executor.Record) bool { return true }
		filterRequired = true
	}

	scan := executor.NewTableScan(executor.TableScanParams{
		ReadView:       s.readView,
		VersionReader:  s.versionReader,
		Table:          tbl,
		SearchMode:     searchMode,
		WhileCondition: whileCond,
	})
	if filterRequired {
		return executor.NewFilter(scan, cond)
	}
	return scan
}

// -------------------------------------------------
// OR 条件の最適化
// -------------------------------------------------

// planForORCondition は OR 条件に対して Union による最適化を試みる
//
// 各 OR ブランチが PK またはセカンダリインデックスを利用できる場合、各ブランチを個別にスキャンし Union で結合する
//
// 最適化できない場合はテーブルスキャン + Filter にフォールバックする
func (s *Search) planForORCondition(tbl *access.Table, expr ast.BinaryExpr, cond func(executor.Record) bool) (executor.Executor, error) {
	tableScanPlan := executor.NewFilter(
		executor.NewTableScan(executor.TableScanParams{
			ReadView:       s.readView,
			VersionReader:  s.versionReader,
			Table:          tbl,
			SearchMode:     access.RecordSearchModeStart{},
			WhileCondition: func(record executor.Record) bool { return true },
		}),
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

	// Union の合計コスト < フルスキャンのコストなら Union を採用
	primaryBTree := btree.NewBTree(tbl.MetaPageId)
	clusterPageReadCost, err := calcPageReadCost(s.bufferPool, primaryBTree)
	if err != nil {
		return nil, err
	}
	fullScanCost := calcFullScanCost(stats, clusterPageReadCost)
	if totalCost < fullScanCost {
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
func (s *Search) planORBranch(tbl *access.Table, branch orBranch, stats *handler.TableStatistics) (executor.Executor, float64, bool) {
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

		// != はレンジ分析の対象外
		if leaf.operator == "!=" {
			continue
		}

		// ユニークスキャン (= 検索) → コスト 1.0
		if leaf.operator == "=" {
			if s.isPKLeadingColumn(leaf.colName) {
				bestLeaf = leaf
				bestCost = calcUniqueScanCost()
				bestPlan = "PK"
				break
			}
			if _, hasIdx := s.tblMeta.GetIndexByColName(leaf.colName); hasIdx {
				bestLeaf = leaf
				bestCost = calcUniqueScanCost()
				bestPlan = "Index"
				break
			}
		}

		// PK レンジスキャン
		if s.isPKLeadingColumn(leaf.colName) {
			primaryBTree := btree.NewBTree(tbl.MetaPageId)
			lowerKey, upperKey, leftIncl, rightIncl := buildRangeKeys(leaf.operator, leaf.literal)
			foundRecords, err := primaryBTree.RecordsInRange(s.bufferPool, lowerKey, upperKey, leftIncl, rightIncl)
			if err != nil {
				return nil, 0, false
			}
			clusterPRC, err := calcPageReadCost(s.bufferPool, primaryBTree)
			if err != nil {
				return nil, 0, false
			}
			readTime := calcReadTimeForClusteredIndex(1, float64(foundRecords), float64(stats.RecordCount), float64(stats.LeafPageCount), clusterPRC)
			cost := calcRangeScanCost(readTime, float64(foundRecords))
			if bestLeaf == nil || cost < bestCost {
				bestLeaf = leaf
				bestCost = cost
				bestPlan = "PK"
			}
		}

		// セカンダリインデックスのレンジスキャン
		idxMeta, hasIndex := s.tblMeta.GetIndexByColName(leaf.colName)
		if hasIndex {
			index, err := tbl.GetUniqueIndexByName(idxMeta.Name)
			if err != nil {
				return nil, 0, false
			}
			indexBTree := btree.NewBTree(index.MetaPageId)
			lowerKey, upperKey, leftIncl, rightIncl := buildRangeKeys(leaf.operator, leaf.literal)
			foundRecords, err := indexBTree.RecordsInRange(s.bufferPool, lowerKey, upperKey, leftIncl, rightIncl)
			if err != nil {
				return nil, 0, false
			}
			idxPRC, err := calcPageReadCost(s.bufferPool, indexBTree)
			if err != nil {
				return nil, 0, false
			}
			readTime := calcReadTimeForSecondaryIndex(1, float64(foundRecords), idxPRC)
			cost := calcRangeScanCost(readTime, float64(foundRecords))
			if bestLeaf == nil || cost < bestCost {
				bestLeaf = leaf
				bestCost = cost
				bestPlan = "Index"
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

// -------------------------------------------------
// その他
// -------------------------------------------------

// buildRangeKeys は演算子とリテラルからレンジ分析用のキーを組み立てる
//
// 非有界側は nil を返す。RecordsInRange は nil を「先頭から」「末尾まで」として扱う
func buildRangeKeys(operator string, literal ast.Literal) (lowerKey, upperKey []byte, leftIncl, rightIncl bool) {
	var encoded []byte
	encode.Encode([][]byte{literal.ToBytes()}, &encoded)

	switch operator {
	case "=":
		return encoded, encoded, true, true
	case ">":
		return encoded, nil, false, true
	case ">=":
		return encoded, nil, true, true
	case "<":
		return nil, encoded, true, false
	case "<=":
		return nil, encoded, true, true
	default:
		return nil, nil, true, true
	}
}

// isPKLeadingColumn は指定カラムがプライマリキーの先頭カラムかどうかを判定する
func (s *Search) isPKLeadingColumn(colName string) bool {
	if s.tblMeta.PKCount == 0 {
		return false
	}
	colMeta, ok := s.tblMeta.GetColByName(colName)
	return ok && colMeta.Pos == 0
}
