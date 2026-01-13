package planner

import (
	"fmt"
	"minesql/internal/executor"
	"minesql/internal/planner/ast/definition"
	"minesql/internal/planner/ast/statement"
)

type Node interface {
	Start() executor.Executor
}

type Planner struct{}

func NewPlanner() *Planner {
	return &Planner{}
}

func (p *Planner) PlanStart(stmt statement.Statement) (executor.Executor, error) {
	switch s := stmt.(type) {
	case *statement.CreateTableStmt:
		cols := []string{}
		pkCols := []string{}
		uk := []struct {
			name string
			cols []string
		}{}
		for _, def := range s.CreateDefinitions {
			switch def := def.(type) {
			case *definition.ColumnDef:
				cols = append(cols, def.ColName)
			case *definition.PrimaryKeyDef:
				if len(pkCols) > 0 {
					return nil, fmt.Errorf("multiple primary keys defined")
				}
				for _, col := range def.Columns {
					pkCols = append(pkCols, col.ColName)
				}
			case *definition.UniqueKeyDef:
				ukCols := []string{}
				for _, col := range def.Columns {
					ukCols = append(ukCols, col.ColName)
				}
				if len(ukCols) == 0 {
					return nil, fmt.Errorf("unique key must have at least one column")
				}
				if len(ukCols) != 1 {
					return nil, fmt.Errorf("only single-column unique keys are supported")
				}
				uk = append(uk, struct {
					name string
					cols []string
				}{
					name: def.KeyName,
					cols: ukCols,
				})
			default:
				continue
			}
		}

		if len(pkCols) == 0 {
			return nil, fmt.Errorf("no primary key defined")
		}

		primaryKeyIdxs := []int{}
		for i, col := range cols {
			for _, pkCol := range pkCols {
				if col == pkCol {
					primaryKeyIdxs = append(primaryKeyIdxs, i)
				}
			}
		}
		if len(primaryKeyIdxs) != len(pkCols) {
			return nil, fmt.Errorf("primary key columns not found in table columns")
		}
		if primaryKeyIdxs[0] != 0 {
			return nil, fmt.Errorf("primary key must be the first column")
		}

		primaryKeyCount := 0
		for i, idx := range primaryKeyIdxs {
			if idx != i {
				return nil, fmt.Errorf("primary key columns must be contiguous starting from the first column")
			}
			primaryKeyCount++
		}

		ukParams := []*executor.IndexParam{}
		for i, col := range cols {
			for _, uniqueKey := range uk {
				if col == uniqueKey.cols[0] {
					ukParams = append(ukParams, &executor.IndexParam{
						Name:         uniqueKey.name,
						SecondaryKey: uint(i),
					})
				}
			}
		}
		if len(ukParams) != len(uk) {
			return nil, fmt.Errorf("unique key columns not found in table columns")
		}

		return executor.NewCreateTable(s.TableName, primaryKeyCount, ukParams), nil
	default:
		return nil, fmt.Errorf("unsupported statement: %T", s)
	}
}
