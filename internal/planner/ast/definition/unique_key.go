package definition

import "minesql/internal/planner/ast/identifier"

type ConstraintUniqueKeyDef struct {
	DefType DefType
	KeyName string
	Columns []identifier.ColumnId
}

func NewConstraintUniqueKeyDef(columns []identifier.ColumnId) *ConstraintUniqueKeyDef {
	return &ConstraintUniqueKeyDef{
		DefType: DefTypeConstraintUniqueKey,
		Columns: columns,
	}
}

func (ukd *ConstraintUniqueKeyDef) definitionNode() {}
