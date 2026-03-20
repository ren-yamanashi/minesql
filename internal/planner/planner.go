package planner

import (
	"fmt"
	"minesql/internal/executor"
	"minesql/internal/planner/ast/node"
	"minesql/internal/planner/ast/statement"
)

func PlanStart(stmt node.ASTNode) (executor.Executor, error) {
	switch s := stmt.(type) {
	case *statement.CreateTableStmt:
		ctn := NewCreateTableNode(s)
		return ctn.Next()
	case *statement.InsertStmt:
		ip := NewInsertPlanner(s)
		return ip.Next()
	case *statement.SelectStmt:
		search := NewSearchPlanner(s.From.TableName, s.Where)
		searchExec, err := search.Next()
		if err != nil {
			return nil, err
		}
		sp := NewSelectPlanner(s, searchExec)
		return sp.Next()
	case *statement.DeleteStmt:
		search := NewSearchPlanner(s.From.TableName, s.Where)
		searchExec, err := search.Next()
		if err != nil {
			return nil, err
		}
		dp := NewDeletePlanner(s, searchExec)
		return dp.Next()
	case *statement.UpdateStmt:
		search := NewSearchPlanner(s.Table.TableName, s.Where)
		searchExec, err := search.Next()
		if err != nil {
			return nil, err
		}
		up := NewUpdatePlanner(s, searchExec)
		return up.Next()
	default:
		return nil, fmt.Errorf("unsupported statement: %T", s)
	}
}
