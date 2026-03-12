package statement

import "minesql/internal/planner/ast/identifier"

type DeleteStmt struct {
	StmtType StmtType
	From     identifier.TableId
	Where    *WhereClause
}

func NewDeleteStmt(from identifier.TableId, where *WhereClause) *DeleteStmt {
	if where == nil {
		where = &WhereClause{
			IsSet: false,
		}
	}
	return &DeleteStmt{
		StmtType: StmtTypeDelete,
		From:     from,
		Where:    where,
	}
}

func (ds *DeleteStmt) statementNode() {}
