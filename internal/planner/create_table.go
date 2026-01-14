package planner

import (
	"errors"
	"fmt"
	"minesql/internal/executor"
	"minesql/internal/planner/ast/definition"
	"minesql/internal/planner/ast/statement"
)

type CreateTableNode struct {
	Stmt *statement.CreateTableStmt
}

func NewCreateTableNode(stmt *statement.CreateTableStmt) *CreateTableNode {
	return &CreateTableNode{
		Stmt: stmt,
	}
}

func (ctn *CreateTableNode) Next() (executor.Executor, error) {
	colIndexMap := map[string]int{} // key: column name, value: column index
	columnNames := []string{}

	var pkDef *definition.PrimaryKeyDef
	var ukDefs []*definition.UniqueKeyDef

	currentColIdx := 0
	for _, def := range ctn.Stmt.CreateDefinitions {
		switch def := def.(type) {
		case *definition.ColumnDef:
			if _, exists := colIndexMap[def.ColName]; exists {
				return nil, errors.New("duplicate column name: " + def.ColName)
			}
			colIndexMap[def.ColName] = currentColIdx
			columnNames = append(columnNames, def.ColName)
			currentColIdx++

		case *definition.PrimaryKeyDef:
			if pkDef != nil {
				return nil, fmt.Errorf("multiple primary keys defined")
			}
			pkDef = def

		case *definition.UniqueKeyDef:
			ukDefs = append(ukDefs, def)
		}
	}

	if pkDef == nil {
		return nil, errors.New("primary key is required")
	}

	if len(pkDef.Columns) > len(columnNames) {
		return nil, errors.New("primary key columns exceed total columns")
	}

	for i, pkCol := range pkDef.Columns {
		idx, exists := colIndexMap[pkCol.ColName]
		if !exists {
			return nil, fmt.Errorf("primary key column '%s' does not exist", pkCol.ColName)
		}
		if idx != i {
			return nil, errors.New("primary key columns must be defined in order starting from the first column")
		}
	}

	uniqueKeyParams := make([]*executor.IndexParam, 0, len(ukDefs))
	for _, ukDef := range ukDefs {
		if len(ukDef.Columns) == 0 {
			return nil, fmt.Errorf("unique key '%s' must have at least one column", ukDef.KeyName)
		}
		if len(ukDef.Columns) != 1 {
			return nil, fmt.Errorf("only single-column unique keys are supported currently")
		}
		idx, exists := colIndexMap[ukDef.Columns[0].ColName]
		if !exists {
			return nil, fmt.Errorf("unique key column '%s' does not exist", ukDef.Columns[0].ColName)
		}
		uniqueKeyParams = append(uniqueKeyParams, &executor.IndexParam{
			Name:         ukDef.KeyName,
			SecondaryKey: uint(idx),
		})
	}

	return executor.NewCreateTable(ctn.Stmt.TableName, len(pkDef.Columns), uniqueKeyParams), nil
}
