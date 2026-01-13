package planner

import (
	"errors"
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
	columnNames := []string{}
	pkColumns := []string{}
	uniqueKeys := []struct {
		name string
		cols []string
	}{}

	for _, def := range ctn.Stmt.CreateDefinitions {
		switch def := def.(type) {
		case *definition.ColumnDef:
			columnNames = append(columnNames, def.ColName)
		case *definition.PrimaryKeyDef:
			if len(pkColumns) > 0 {
				return nil, errors.New("multiple primary keys defined")
			}
			for _, col := range def.Columns {
				pkColumns = append(pkColumns, col.ColName)
			}
		case *definition.UniqueKeyDef:
			uniqueKeyColumnNames := []string{}
			if len(def.Columns) == 0 {
				return nil, errors.New("unique key must have at least one column")
			}
			if len(uniqueKeyColumnNames) != 1 {
				return nil, errors.New("only single-column unique keys are supported")
			}
			for _, col := range def.Columns {
				uniqueKeyColumnNames = append(uniqueKeyColumnNames, col.ColName)
			}
			uniqueKeys = append(uniqueKeys, struct {
				name string
				cols []string
			}{
				name: def.KeyName,
				cols: uniqueKeyColumnNames,
			})
		default:
			continue
		}
	}

	if len(pkColumns) == 0 {
		return nil, errors.New("primary key is required")
	}

	primaryKeyIndexes := []int{}
	for i, colName := range columnNames {
		for _, pkCol := range pkColumns {
			if colName == pkCol {
				primaryKeyIndexes = append(primaryKeyIndexes, i)
			}
		}
	}
	if len(primaryKeyIndexes) != len(pkColumns) {
		return nil, errors.New("primary key columns not found in table columns")
	}
	if primaryKeyIndexes[0] != 0 {
		return nil, errors.New("the first column must be part of the primary key")
	}

	primaryKeyCount := 0
	for i, idx := range primaryKeyIndexes {
		if idx != i {
			return nil, errors.New("primary key columns must be contiguous starting from the first column")
		}
		primaryKeyCount++
	}

	uniqueKeyParams := []*executor.IndexParam{}
	for i, colName := range columnNames {
		for _, uniqueKey := range uniqueKeys {
			if colName == uniqueKey.cols[0] {
				uniqueKeyParams = append(uniqueKeyParams, &executor.IndexParam{
					Name:         uniqueKey.name,
					SecondaryKey: uint(i),
				})
			}
		}
	}
	if len(uniqueKeyParams) != len(uniqueKeys) {
		return nil, errors.New("unique key columns not found in table columns")
	}

	return executor.NewCreateTable(ctn.Stmt.TableName, primaryKeyCount, uniqueKeyParams), nil
}
