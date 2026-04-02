package planner

import (
	"errors"
	"fmt"
	"minesql/internal/ast"
	"minesql/internal/executor"
	"minesql/internal/storage/handler"
)

func PlanCreateTable(stmt *ast.CreateTableStmt) (executor.Executor, error) {
	colIndexMap := map[string]int{} // key: column name, value: column index
	colParams := []handler.CreateColumnParam{}

	var pkDef *ast.ConstraintPrimaryKeyDef
	var ukDefs []*ast.ConstraintUniqueKeyDef
	ukKeyNames := map[string]bool{} // 登録済みのユニークキー名
	ukColNames := map[string]bool{} // 登録済みのユニークキーカラム名
	currentColIdx := 0

	// テーブル定義の検証とカラム・インデックスの収集
	for _, def := range stmt.CreateDefinitions {
		switch def := def.(type) {
		case *ast.ColumnDef:
			// カラム定義の検証
			if _, exists := colIndexMap[def.ColName]; exists {
				return nil, errors.New("duplicate column name: " + def.ColName)
			}
			colIndexMap[def.ColName] = currentColIdx
			currentColIdx++
			colParams = append(colParams, handler.CreateColumnParam{
				Name: def.ColName,
				Type: handler.ColumnType(def.DataType),
			})

		case *ast.ConstraintPrimaryKeyDef:
			// プライマリキー定義の検証
			if pkDef != nil {
				return nil, errors.New("multiple primary keys defined")
			}
			pkDef = def

		case *ast.ConstraintUniqueKeyDef:
			// ユニークキー定義の検証
			if _, exists := ukKeyNames[def.KeyName]; exists {
				return nil, errors.New("duplicate unique key name: " + def.KeyName)
			}
			if _, exists := ukColNames[def.Column.ColName]; exists {
				return nil, errors.New("column '" + def.Column.ColName + "' cannot be part of multiple unique keys")
			}
			ukKeyNames[def.KeyName] = true
			ukColNames[def.Column.ColName] = true
			ukDefs = append(ukDefs, def)
		}
	}

	// カラム定義がない場合はエラー
	if len(colParams) == 0 {
		return nil, errors.New("table must have at least one column")
	}

	pkCount, err := getPkCount(pkDef, colIndexMap)
	if err != nil {
		return nil, err
	}

	uniqueKeyParams, err := getUkParams(ukDefs, colIndexMap)
	if err != nil {
		return nil, err
	}

	return executor.NewCreateTable(stmt.TableName, uint8(pkCount), uniqueKeyParams, colParams), nil
}

// getPkCount はプライマリキーのカラム定義を検証し、プライマリキーのカラム数を返す
//
// 以下の場合はエラーを返す
//   - プライマリキー定義がない場合
//   - プライマリキーにカラムが定義されていない場合
//   - プライマリキーのカラム数がテーブルのカラム数を超える場合
//   - プライマリキーのカラムが順序通りに (最初の列から順番に) 定義されていない場合
//   - プライマリキーのカラムがテーブルのカラムに存在しない場合
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

// getUkParams はユニークキーの定義を検証し、ユニークキーのパラメータを返す
//
// 以下の場合はエラーを返す
//   - ユニークキーのカラムがテーブルのカラムに存在しない場合
func getUkParams(ukDefs []*ast.ConstraintUniqueKeyDef, colIndexMap map[string]int) ([]handler.CreateIndexParam, error) {
	uniqueKeyParams := make([]handler.CreateIndexParam, 0, len(ukDefs))
	for _, ukDef := range ukDefs {
		idx, exists := colIndexMap[ukDef.Column.ColName]
		if !exists {
			return nil, fmt.Errorf("unique key column '%s' does not exist", ukDef.Column.ColName)
		}
		uniqueKeyParams = append(uniqueKeyParams, handler.CreateIndexParam{
			Name:    ukDef.KeyName,
			ColName: ukDef.Column.ColName,
			UkIdx:   uint16(idx),
		})
	}
	return uniqueKeyParams, nil
}
