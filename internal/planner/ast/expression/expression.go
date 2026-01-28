package expression

import "minesql/internal/planner/ast/node"

type ExprType string

const (
	ExprTypeBinary ExprType = "BINARY"
)

type Expression interface {
	node.ASTNode
	expressionNode()
}
