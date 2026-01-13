package definition

import "minesql/internal/planner/ast/identifier"

type PrimaryKeyDef struct {
	DefType DefType
	Columns []identifier.ColumnId
}

func NewConstraintPrimaryKeyDef(columns []identifier.ColumnId) *PrimaryKeyDef {
	return &PrimaryKeyDef{
		DefType: DefTypeConstraintPrimaryKey,
		Columns: columns,
	}
}

func (pkd *PrimaryKeyDef) definitionNode() {}
