package handler

import (
	"fmt"
	"minesql/internal/storage/access"
	"minesql/internal/storage/dictionary"
	"minesql/internal/storage/log"
)

// GetTable はテーブルメタデータからテーブルを取得し、UndoManager をセットして返す
func (h *Handler) GetTable(tableName string) (*access.Table, error) {
	tblMeta, ok := h.Catalog.GetTableMetaByName(tableName)
	if !ok {
		return nil, fmt.Errorf("table %s not found", tableName)
	}
	return buildTable(tblMeta, h.undoLog, h.redoLog)
}

// buildAllTables はカタログに登録されている全テーブルを構築する
func buildAllTables(catalog *dictionary.Catalog, undoLog *access.UndoManager, redoLog *log.RedoLog) []*access.Table {
	var tables []*access.Table
	for _, tblMeta := range catalog.GetAllTables() {
		tbl, err := buildTable(tblMeta, undoLog, redoLog)
		if err != nil {
			continue
		}
		tables = append(tables, tbl)
	}
	return tables
}

// buildTable はテーブルメタデータから Table を構築する
func buildTable(tblMeta *dictionary.TableMeta, undoLog *access.UndoManager, redoLog *log.RedoLog) (*access.Table, error) {
	var uniqueIndexes []*access.UniqueIndex
	for _, idxMeta := range tblMeta.Indexes {
		if idxMeta.Type == dictionary.IndexTypeUnique {
			colMeta, ok := tblMeta.GetColByName(idxMeta.ColName)
			if !ok {
				return nil, fmt.Errorf("column %s not found in table %s", idxMeta.ColName, tblMeta.Name)
			}
			ui := access.NewUniqueIndex(idxMeta.Name, idxMeta.ColName, idxMeta.DataMetaPageId, colMeta.Pos, tblMeta.PKCount)
			uniqueIndexes = append(uniqueIndexes, ui)
		}
	}

	tbl := access.NewTable(tblMeta.Name, tblMeta.DataMetaPageId, tblMeta.PKCount, uniqueIndexes, undoLog, redoLog)
	return &tbl, nil
}
