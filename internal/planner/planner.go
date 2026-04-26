package planner

import (
	"fmt"
	"minesql/internal/ast"
	"minesql/internal/executor"
	"minesql/internal/storage/handler"
)

// ColumnMeta は結果セットのカラムメタデータ
type ColumnMeta struct {
	TableName string
	ColName   string
}

// PlanResult はプランナーの実行結果
type PlanResult struct {
	Exec    executor.Executor // クエリ実行のための Executor
	Columns []ColumnMeta      // SELECT の場合のみ設定。それ以外は nil
}

func Start(trxId handler.TrxId, stmt ast.Statement) (*PlanResult, error) {
	switch s := stmt.(type) {
	case *ast.CreateTableStmt:
		exec, err := PlanCreateTable(s)
		return &PlanResult{Exec: exec}, err
	case *ast.InsertStmt:
		exec, err := PlanInsert(trxId, s)
		return &PlanResult{Exec: exec}, err
	case *ast.SelectStmt:
		return PlanSelect(trxId, s)
	case *ast.DeleteStmt:
		exec, err := PlanDelete(trxId, s)
		return &PlanResult{Exec: exec}, err
	case *ast.UpdateStmt:
		exec, err := PlanUpdate(trxId, s)
		return &PlanResult{Exec: exec}, err
	default:
		return nil, fmt.Errorf("unsupported statement: %T", s)
	}
}
