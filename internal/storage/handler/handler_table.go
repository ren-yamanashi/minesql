package handler

import (
	"fmt"
	"minesql/internal/storage/access"
	"minesql/internal/storage/dictionary"
)

// GetTable はテーブルメタデータからテーブルを取得し、UndoLog をセットして返す
func (h *Handler) GetTable(tableName string) (*access.Table, error) {
	tblMeta, ok := h.Catalog.GetTableMetaByName(tableName)
	if !ok {
		return nil, fmt.Errorf("table %s not found", tableName)
	}

	// ユニークインデックスを構築
	var uniqueIndexes []*access.UniqueIndex
	for _, idxMeta := range tblMeta.Indexes {
		if idxMeta.Type == dictionary.IndexTypeUnique {
			colMeta, ok := tblMeta.GetColByName(idxMeta.ColName)
			if !ok {
				return nil, fmt.Errorf("column %s not found in table %s", idxMeta.ColName, tableName)
			}
			ui := access.NewUniqueIndex(idxMeta.Name, idxMeta.ColName, idxMeta.DataMetaPageId, colMeta.Pos, tblMeta.PKCount)
			uniqueIndexes = append(uniqueIndexes, ui)
		}
	}

	tbl := access.NewTable(tblMeta.Name, tblMeta.DataMetaPageId, tblMeta.PKCount, uniqueIndexes, h.undoLog, h.redoLog)
	return &tbl, nil
}
