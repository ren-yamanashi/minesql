package planner

import (
	"minesql/internal/executor"
	"minesql/internal/planner/ast/statement"
)

type SelectPlanner struct {
	Stmt          *statement.SelectStmt
	InnerExecutor executor.Executor
}

func NewSelectPlanner(stmt *statement.SelectStmt, innerExecutor executor.Executor) *SelectPlanner {
	return &SelectPlanner{
		Stmt:          stmt,
		InnerExecutor: innerExecutor,
	}
}

func (sp *SelectPlanner) Next() (executor.Executor, error) {
	// 現時点では検索 Executor をそのまま返す
	// 将来的にカラム射影などを追加する場合はここに処理を追加する
	return sp.InnerExecutor, nil
}
