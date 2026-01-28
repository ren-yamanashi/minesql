package definition

import "minesql/internal/planner/ast/identifier"

type ConstraintUniqueKeyDef struct {
	DefType DefType
	KeyName string
	Column identifier.ColumnId
}

func NewConstraintUniqueKeyDef(column identifier.ColumnId) *ConstraintUniqueKeyDef {
	return &ConstraintUniqueKeyDef{
		DefType: DefTypeConstraintUniqueKey,
		Column: column,
	}
}

func (ukd *ConstraintUniqueKeyDef) definitionNode() {}
