package planner

import (
	"errors"
	"fmt"
	"minesql/internal/executor"
	"minesql/internal/planner/ast/definition"
	"minesql/internal/planner/ast/statement"
	"minesql/internal/storage/access/catalog"
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
	colParams := []*executor.ColumnParam{}

	var pkDef *definition.ConstraintPrimaryKeyDef
	var ukDefs []*definition.ConstraintUniqueKeyDef

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
			colParams = append(colParams, &executor.ColumnParam{
				Name: def.ColName,
				Type: catalog.ColumnType(def.DataType),
			})

		case *definition.ConstraintPrimaryKeyDef:
			if pkDef != nil {
				return nil, fmt.Errorf("multiple primary keys defined")
			}
			pkDef = def

		case *definition.ConstraintUniqueKeyDef:
			ukDefs = append(ukDefs, def)
		}
	}

	if err := validatePkDef(pkDef, colIndexMap); err != nil {
		return nil, err
	}

	uniqueKeyParams, err := getUkParams(ukDefs, colIndexMap)
	if err != nil {
		return nil, err
	}

	return executor.NewCreateTable(ctn.Stmt.TableName, len(pkDef.Columns), uniqueKeyParams, colParams), nil
}

func validatePkDef(pkDef *definition.ConstraintPrimaryKeyDef, colIndexMap map[string]int) error {
	if pkDef == nil {
		return errors.New("primary key is required")
	}
	if len(pkDef.Columns) == 0 {
		return errors.New("primary key must have at least one column")
	}
	if len(pkDef.Columns) > len(colIndexMap) {
		return errors.New("primary key columns exceed total columns")
	}

	for i, pkCol := range pkDef.Columns {
		idx, exists := colIndexMap[pkCol.ColName]
		if !exists {
			return fmt.Errorf("primary key column '%s' does not exist", pkCol.ColName)
		}
		if idx != i {
			return errors.New("primary key columns must be defined in order starting from the first column")
		}
	}
	return nil
}

func getUkParams(ukDefs []*definition.ConstraintUniqueKeyDef, colIndexMap map[string]int) ([]*executor.IndexParam, error) {
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
	return uniqueKeyParams, nil
}
