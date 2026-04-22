package planner

import (
	"errors"
	"fmt"
	"minesql/internal/ast"
	"minesql/internal/executor"
	"minesql/internal/storage/dictionary"
	"minesql/internal/storage/handler"
)

// PlanCreateTable は CREATE TABLE 文の実行計画を構築する
func PlanCreateTable(stmt *ast.CreateTableStmt) (executor.Executor, error) {
	colIndexMap := map[string]int{} // key: column name, value: column index
	colParams := []handler.CreateColumnParam{}

	var pkDef *ast.ConstraintPrimaryKeyDef
	var ukDefs []*ast.ConstraintUniqueKeyDef
	var keyDefs []*ast.ConstraintKeyDef
	var fkDefs []*ast.ConstraintForeignKeyDef
	idxKeyNames := map[string]bool{} // 登録済みのインデックス名 (UK/KEY/FK 名との重複チェックにも使用)
	idxColNames := map[string]bool{} // 登録済みのインデックスカラム名 (UK/KEY 共通)
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
			if _, exists := idxKeyNames[def.KeyName]; exists {
				return nil, errors.New("duplicate index name: " + def.KeyName)
			}
			if _, exists := idxColNames[def.Column.ColName]; exists {
				return nil, errors.New("column '" + def.Column.ColName + "' cannot have multiple indexes")
			}
			idxKeyNames[def.KeyName] = true
			idxColNames[def.Column.ColName] = true
			ukDefs = append(ukDefs, def)

		case *ast.ConstraintKeyDef:
			// 非ユニークキー定義の検証
			if _, exists := idxKeyNames[def.KeyName]; exists {
				return nil, errors.New("duplicate index name: " + def.KeyName)
			}
			if _, exists := idxColNames[def.Column.ColName]; exists {
				return nil, errors.New("column '" + def.Column.ColName + "' cannot have multiple indexes")
			}
			idxKeyNames[def.KeyName] = true
			idxColNames[def.Column.ColName] = true
			keyDefs = append(keyDefs, def)

		case *ast.ConstraintForeignKeyDef:
			fkDefs = append(fkDefs, def)
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

	idxParams, err := getIndexParams(ukDefs, keyDefs, colIndexMap)
	if err != nil {
		return nil, err
	}

	constraintParams, err := getForeignKeyParams(stmt.TableName, fkDefs, colIndexMap, idxKeyNames, idxColNames)
	if err != nil {
		return nil, err
	}

	return executor.NewCreateTable(stmt.TableName, uint8(pkCount), idxParams, colParams, constraintParams), nil
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

// getIndexParams はユニークキーと非ユニークキーの定義を検証し、インデックスのパラメータを返す
//
// 以下の場合はエラーを返す
//   - インデックスのカラムがテーブルのカラムに存在しない場合
func getIndexParams(ukDefs []*ast.ConstraintUniqueKeyDef, keyDefs []*ast.ConstraintKeyDef, colIndexMap map[string]int) ([]handler.CreateIndexParam, error) {
	params := make([]handler.CreateIndexParam, 0, len(ukDefs)+len(keyDefs))

	// ユニークキー
	for _, ukDef := range ukDefs {
		idx, exists := colIndexMap[ukDef.Column.ColName]
		if !exists {
			return nil, fmt.Errorf("unique key column '%s' does not exist", ukDef.Column.ColName)
		}
		params = append(params, handler.CreateIndexParam{
			Name:    ukDef.KeyName,
			ColName: ukDef.Column.ColName,
			ColIdx:  uint16(idx),
			Unique:  true,
		})
	}

	// 非ユニークキー
	for _, keyDef := range keyDefs {
		idx, exists := colIndexMap[keyDef.Column.ColName]
		if !exists {
			return nil, fmt.Errorf("key column '%s' does not exist", keyDef.Column.ColName)
		}
		params = append(params, handler.CreateIndexParam{
			Name:    keyDef.KeyName,
			ColName: keyDef.Column.ColName,
			ColIdx:  uint16(idx),
			Unique:  false,
		})
	}

	return params, nil
}

// getForeignKeyParams は外部キー定義を検証し、外部キー制約のパラメータを返す
func getForeignKeyParams(tableName string, fkDefs []*ast.ConstraintForeignKeyDef, colIndexMap map[string]int, idxKeyNames map[string]bool, idxColNames map[string]bool) ([]handler.CreateConstraintParam, error) {
	if len(fkDefs) == 0 {
		return nil, nil
	}

	hdl := handler.Get()
	params := make([]handler.CreateConstraintParam, 0, len(fkDefs))
	fkNames := map[string]bool{} // 同一 CREATE TABLE 内で処理済みの FK 名

	for _, fkDef := range fkDefs {
		// FK 名が同一 CREATE TABLE 内で重複していないか
		if fkNames[fkDef.KeyName] {
			return nil, fmt.Errorf("duplicate foreign key constraint name: '%s'", fkDef.KeyName)
		}

		// FK 名が全テーブルを通じて一意であるか
		for _, tblMeta := range hdl.Catalog.GetAllTables() {
			for _, con := range tblMeta.Constraints {
				if con.ConstraintName == fkDef.KeyName {
					return nil, fmt.Errorf("duplicate foreign key constraint name: '%s'", fkDef.KeyName)
				}
			}
		}
		if _, exists := idxKeyNames[fkDef.KeyName]; exists {
			return nil, fmt.Errorf("duplicate constraint name: '%s'", fkDef.KeyName)
		}

		fkNames[fkDef.KeyName] = true

		// FK カラムがテーブルのカラム定義に存在するか
		if _, exists := colIndexMap[fkDef.Column.ColName]; !exists {
			return nil, fmt.Errorf("foreign key column '%s' does not exist", fkDef.Column.ColName)
		}

		// 自己参照でないか
		if fkDef.RefTable == tableName {
			return nil, fmt.Errorf("self-referencing foreign key is not supported")
		}

		// 参照先テーブルがカタログに存在するか
		refTableMeta, ok := hdl.Catalog.GetTableMetaByName(fkDef.RefTable)
		if !ok {
			return nil, fmt.Errorf("referenced table '%s' does not exist", fkDef.RefTable)
		}

		// 参照先カラムが参照先テーブルに存在するか
		refCol, ok := refTableMeta.GetColByName(fkDef.RefColumn)
		if !ok {
			return nil, fmt.Errorf("referenced column '%s' does not exist in table '%s'", fkDef.RefColumn, fkDef.RefTable)
		}

		// 参照先カラムに PK または UNIQUE KEY があるか
		isPK := refCol.Pos < uint16(refTableMeta.PKCount)
		refIdx, hasIdx := refTableMeta.GetIndexByColName(fkDef.RefColumn)
		hasUniqueIdx := hasIdx && refIdx.Type == dictionary.IndexTypeUnique
		if !isPK && !hasUniqueIdx {
			return nil, fmt.Errorf("referenced column '%s' in table '%s' must be a primary key or have a unique index", fkDef.RefColumn, fkDef.RefTable)
		}

		// FK カラムにインデックスがあるか (同一 CREATE TABLE 内で定義されている必要あり)
		if _, exists := idxColNames[fkDef.Column.ColName]; !exists {
			return nil, fmt.Errorf("foreign key column '%s' must have an index", fkDef.Column.ColName)
		}

		params = append(params, handler.CreateConstraintParam{
			ConstraintName: fkDef.KeyName,
			ColName:        fkDef.Column.ColName,
			RefTableName:   fkDef.RefTable,
			RefColName:     fkDef.RefColumn,
		})
	}

	return params, nil
}
