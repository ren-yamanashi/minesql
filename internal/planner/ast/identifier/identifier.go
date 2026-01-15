package identifier

import "minesql/internal/planner/ast/node"

type IdType string

const (
	IdTypeColumn IdType = "column"
	IdTypeTable  IdType = "table"
)

type Identifier interface {
	node.ASTNode
	identifierNode()
}
