package definition

import "minesql/internal/planner/ast/node"

type DefType string

const (
	DefTypeConstraintPrimaryKey DefType = "constraint primary key"
	DefTypeConstraintUniqueKey  DefType = "constraint unique key"
	DefTypeColumn               DefType = "column"
)

type Definition interface {
	node.ASTNode
	definitionNode()
}
