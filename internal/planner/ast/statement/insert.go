package statement

import (
	"minesql/internal/planner/ast/identifier"
	"minesql/internal/planner/ast/literal"
)

type InsertStmt struct {
	StmtType StmtType
	Table    identifier.TableId
	Cols     []identifier.ColumnId
	Values   [][]literal.Literal
}

func NewInsertStmt(table identifier.TableId, cols []identifier.ColumnId, values [][]literal.Literal) *InsertStmt {
	return &InsertStmt{
		StmtType: StmtTypeInsert,
		Table:    table,
		Cols:     cols,
		Values:   values,
	}
}

func (is *InsertStmt) statementNode() {}
