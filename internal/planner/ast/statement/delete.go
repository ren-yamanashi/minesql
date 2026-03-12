package statement

import "minesql/internal/planner/ast/identifier"

type DeleteStmt struct {
	StmtType StmtType
	From     identifier.TableId
	Where    *WhereClause
}

func (ds *DeleteStmt) statementNode() {}
