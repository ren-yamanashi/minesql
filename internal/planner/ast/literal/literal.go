package literal

import "minesql/internal/planner/ast/node"

type LiteralType string

const (
	LiteralTypeNumber LiteralType = "number"
	LiteralTypeString LiteralType = "string"
)

type Literal interface {
	node.ASTNode
	literalNode()
}
