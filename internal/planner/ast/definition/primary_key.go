package definition

import "minesql/internal/planner/ast/identifier"

type ConstraintPrimaryKeyDef struct {
	DefType DefType
	Columns []identifier.ColumnId
}

func NewConstraintPrimaryKeyDef(columns []identifier.ColumnId) *ConstraintPrimaryKeyDef {
	return &ConstraintPrimaryKeyDef{
		DefType: DefTypeConstraintPrimaryKey,
		Columns: columns,
	}
}

func (pkd *ConstraintPrimaryKeyDef) definitionNode() {}
