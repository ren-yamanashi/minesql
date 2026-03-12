package statement

import "minesql/internal/planner/ast/node"

const (
	StmtTypeCreate StmtType = "create"
	StmtTypeInsert StmtType = "insert"
	StmtTypeSelect StmtType = "select"
	StmtTypeUpdate StmtType = "update"
	StmtTypeDelete StmtType = "delete"
)

type StmtType string

type Statement interface {
	node.ASTNode
	statementNode()
}
