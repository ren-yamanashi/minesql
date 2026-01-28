package literal

import "minesql/internal/planner/ast/node"

type LiteralType string

const (
	LiteralTypeString LiteralType = "string"
)

type Literal interface {
	node.ASTNode
	ToString() string
	ToBytes() []byte
	literalNode()
}
