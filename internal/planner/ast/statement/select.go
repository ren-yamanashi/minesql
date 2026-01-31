package statement

import (
	"minesql/internal/planner/ast/expression"
	"minesql/internal/planner/ast/identifier"
)

type SelectStmt struct {
	StmtType StmtType
	From     identifier.TableId
	Where    *WhereClause
}

func NewSelectStmt(from identifier.TableId, where *WhereClause) *SelectStmt {
	if where == nil {
		where = &WhereClause{
			IsSet: false,
		}
	}
	return &SelectStmt{
		StmtType: StmtTypeSelect,
		From:     from,
		Where:    where,
	}
}

func (ss *SelectStmt) statementNode() {}

type WhereClause struct {
	Condition expression.Expression
	IsSet     bool
}

func NewWhereClause(condition expression.Expression) *WhereClause {
	return &WhereClause{
		Condition: condition,
		IsSet:     true,
	}
}