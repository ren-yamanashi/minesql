package planner

import (
	"errors"
	"fmt"
	"minesql/internal/ast"
	"minesql/internal/catalog"
	"minesql/internal/executor"
)

type CreateTable struct {
	Stmt *ast.CreateTableStmt
}

func NewCreateTable(stmt *ast.CreateTableStmt) *CreateTable {
	return &CreateTable{
		Stmt: stmt,
	}
}

func (ctn *CreateTable) Build() (executor.Mutator, error) {
	colIndexMap := map[string]int{} // key: column name, value: column index
	colParams := []*executor.ColumnParam{}

	var pkDef *ast.ConstraintPrimaryKeyDef
	var ukDefs []*ast.ConstraintUniqueKeyDef

	currentColIdx := 0
	for _, def := range ctn.Stmt.CreateDefinitions {
		switch def := def.(type) {
		case *ast.ColumnDef:
			if _, exists := colIndexMap[def.ColName]; exists {
				return nil, errors.New("duplicate column name: " + def.ColName)
			}
			colIndexMap[def.ColName] = currentColIdx
			currentColIdx++
			colParams = append(colParams, &executor.ColumnParam{
				Name: def.ColName,
				Type: catalog.ColumnType(def.DataType),
			})

		case *ast.ConstraintPrimaryKeyDef:
			if pkDef != nil {
				return nil, fmt.Errorf("multiple primary keys defined")
			}
			pkDef = def

		case *ast.ConstraintUniqueKeyDef:
			ukDefs = append(ukDefs, def)
		}
	}

	pkCount, err := getPkCount(pkDef, colIndexMap)
	if err != nil {
		return nil, err
	}

	uniqueKeyParams, err := getUkParams(ukDefs, colIndexMap)
	if err != nil {
		return nil, err
	}

	return executor.NewCreateTable(ctn.Stmt.TableName, uint8(pkCount), uniqueKeyParams, colParams), nil
}

// プライマリキーのカラム定義を検証し、プライマリキーのカラム数を返す
// エラーの場合は、`-1, error` を返し、正常な場合は `pkCount, nil` を返す
func getPkCount(pkDef *ast.ConstraintPrimaryKeyDef, colIndexMap map[string]int) (int, error) {
	if pkDef == nil {
		return -1, errors.New("primary key is required")
	}
	if len(pkDef.Columns) == 0 {
		return -1, errors.New("primary key must have at least one column")
	}
	if len(pkDef.Columns) > len(colIndexMap) {
		return -1, errors.New("primary key columns exceed total columns")
	}

	for i, pkCol := range pkDef.Columns {
		idx, exists := colIndexMap[pkCol.ColName]
		if !exists {
			return -1, fmt.Errorf("primary key column '%s' does not exist", pkCol.ColName)
		}
		if idx != i {
			return -1, errors.New("primary key columns must be defined in order starting from the first column")
		}
	}
	return len(pkDef.Columns), nil
}

func getUkParams(ukDefs []*ast.ConstraintUniqueKeyDef, colIndexMap map[string]int) ([]*executor.IndexParam, error) {
	uniqueKeyParams := make([]*executor.IndexParam, 0, len(ukDefs))
	for _, ukDef := range ukDefs {
		idx, exists := colIndexMap[ukDef.Column.ColName]
		if !exists {
			return nil, fmt.Errorf("unique key column '%s' does not exist", ukDef.Column.ColName)
		}
		uniqueKeyParams = append(uniqueKeyParams, &executor.IndexParam{
			Name:         ukDef.KeyName,
			ColName:      ukDef.Column.ColName,
			SecondaryKey: uint16(idx),
		})
	}
	return uniqueKeyParams, nil
}
