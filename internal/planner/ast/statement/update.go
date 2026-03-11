package statement

import (
	"minesql/internal/planner/ast/identifier"
	"minesql/internal/planner/ast/literal"
)

type SetClause struct {
	Column identifier.ColumnId
	Value  literal.Literal
}

type UpdateStmt struct {
	StmtType   StmtType
	Table      identifier.TableId
	SetClauses []*SetClause
	Where      *WhereClause
}

func (us *UpdateStmt) statementNode() {}
