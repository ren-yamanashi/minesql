package definition

import "minesql/internal/planner/ast/identifier"

type UniqueKeyDef struct {
	DefType DefType
	KeyName string
	Columns []identifier.ColumnId
}

func NewConstraintUniqueKeyDef(columns []identifier.ColumnId) *UniqueKeyDef {
	return &UniqueKeyDef{
		DefType: DefTypeConstraintUniqueKey,
		Columns: columns,
	}
}

func (ukd *UniqueKeyDef) definitionNode() {}
