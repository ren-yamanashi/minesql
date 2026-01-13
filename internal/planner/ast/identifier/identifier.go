package identifier

import "minesql/internal/planner/ast/node"

type IdType string

const (
	TypeColumn IdType = "column"
)

type Identifier interface {
	node.ASTNode
	identifierNode()
}
