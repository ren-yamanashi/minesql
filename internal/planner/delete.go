package planner

import "minesql/internal/planner/ast/statement"

type DeletePlanner struct {
	Stmt *statement.DeleteStmt
}
