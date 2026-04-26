package planner

import (
	"fmt"

	"github.com/ren-yamanashi/minesql/internal/ast"
	"github.com/ren-yamanashi/minesql/internal/storage/handler"
)

// joinedColumn は結合後のレコード内でのカラム位置情報
type joinedColumn struct {
	tableName string
	colName   string
	pos       int // 結合レコード内の位置
}

// resolveJoinedColumns は参加テーブルの全カラムを結合順に並べ、位置マッピングを返す
//
// 例: users(id, name, age) + orders(id, user_id, amount) の場合:
//
//	[{users, id, 0}, {users, name, 1}, {users, age, 2},
//	 {orders, id, 3}, {orders, user_id, 4}, {orders, amount, 5}]
func resolveJoinedColumns(tables []*handler.TableMetadata) []joinedColumn {
	var columns []joinedColumn
	pos := 0
	for _, tbl := range tables {
		for _, col := range tbl.GetSortedCols() {
			columns = append(columns, joinedColumn{
				tableName: tbl.Name,
				colName:   col.Name,
				pos:       pos,
			})
			pos++
		}
	}
	return columns
}

// findColumnPos は修飾名 (table.column) または非修飾名 (column) から結合レコード内の位置を返す
//
//   - 修飾名: tableName + colName で一意に特定
//   - 非修飾名: tableName 空 → 全テーブルから検索、曖昧な場合はエラー
func findColumnPos(columns []joinedColumn, tableName, colName string) (int, error) {
	if tableName != "" {
		// 修飾名: テーブル名 + カラム名で一意特定
		for _, col := range columns {
			if col.tableName == tableName && col.colName == colName {
				return col.pos, nil
			}
		}
		return -1, fmt.Errorf("column %s.%s not found in joined tables", tableName, colName)
	}

	// 非修飾名: 全テーブルから検索
	found := -1
	for _, col := range columns {
		if col.colName == colName {
			if found != -1 {
				return -1, fmt.Errorf("ambiguous column name: %s", colName)
			}
			found = col.pos
		}
	}
	if found == -1 {
		return -1, fmt.Errorf("column %s not found in joined tables", colName)
	}
	return found, nil
}

// resolveSelectColumns は単一テーブルの SELECT カラムをカラム位置に変換する
//
// columns が nil の場合は SELECT * (全カラム)
func resolveSelectColumns(columns []ast.ColumnId, tables []*handler.TableMetadata) ([]uint16, error) {
	if len(columns) == 0 {
		// SELECT *: 全カラムの位置を返す
		var pos []uint16
		for _, tbl := range tables {
			for _, col := range tbl.GetSortedCols() {
				pos = append(pos, col.Pos)
			}
		}
		return pos, nil
	}

	// 指定カラムの位置を解決
	joined := resolveJoinedColumns(tables)
	pos := make([]uint16, 0, len(columns))
	for _, col := range columns {
		p, err := findColumnPos(joined, col.TableName, col.ColName)
		if err != nil {
			return nil, err
		}
		pos = append(pos, uint16(p))
	}
	return pos, nil
}

// resolveSelectColumnsForJoin は JOIN 後の結合レコードに対する SELECT カラムをカラム位置に変換する
//
// columns が nil の場合は SELECT * (全カラム)
func resolveSelectColumnsForJoin(columns []ast.ColumnId, joinedCols []joinedColumn, totalColCount int) ([]uint16, error) {
	if len(columns) == 0 {
		// SELECT *: 全カラムの位置を返す
		pos := make([]uint16, totalColCount)
		for i := range pos {
			pos[i] = uint16(i)
		}
		return pos, nil
	}

	// 指定カラムの位置を解決
	pos := make([]uint16, 0, len(columns))
	for _, col := range columns {
		p, err := findColumnPos(joinedCols, col.TableName, col.ColName)
		if err != nil {
			return nil, err
		}
		pos = append(pos, uint16(p))
	}
	return pos, nil
}
