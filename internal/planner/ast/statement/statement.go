package statement

import "minesql/internal/planner/ast/node"

const (
	StmtTypeCreate StmtType = "create"
)

type StmtType string

type Statement interface {
	node.ASTNode
	statementNode()
}
