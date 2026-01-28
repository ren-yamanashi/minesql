package statement

import (
	"minesql/internal/planner/ast/expression"
	"minesql/internal/planner/ast/identifier"
)

type WhereClause struct {
	Condition expression.Expression
	IsSet     bool
}

type SelectStmt struct {
	StmtType StmtType
	From     identifier.TableId
	Where    WhereClause
}

func NewSelectStmt(from identifier.TableId, where WhereClause) *SelectStmt {
	return &SelectStmt{
		StmtType: StmtTypeSelect,
		From:     from,
		Where:    where,
	}
}

func (ss *SelectStmt) statementNode() {}