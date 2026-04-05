package handler

import (
	"fmt"
	"minesql/internal/storage/dictionary"
	"minesql/internal/storage/transaction"
)

// GetTable はテーブルメタデータからテーブルを取得し、UndoLog をセットして返す
func (h *Handler) GetTable(tableName string) (*transaction.Table, error) {
	tblMeta, ok := h.Catalog.GetTableMetaByName(tableName)
	if !ok {
		return nil, fmt.Errorf("table %s not found", tableName)
	}

	// ユニークインデックスを構築
	var uniqueIndexes []*transaction.UniqueIndex
	for _, idxMeta := range tblMeta.Indexes {
		if idxMeta.Type == dictionary.IndexTypeUnique {
			colMeta, ok := tblMeta.GetColByName(idxMeta.ColName)
			if !ok {
				return nil, fmt.Errorf("column %s not found in table %s", idxMeta.ColName, tableName)
			}
			ui := transaction.NewUniqueIndex(idxMeta.Name, idxMeta.ColName, idxMeta.DataMetaPageId, colMeta.Pos, tblMeta.PKCount)
			uniqueIndexes = append(uniqueIndexes, ui)
		}
	}

	tbl := transaction.NewTable(tblMeta.Name, tblMeta.DataMetaPageId, tblMeta.PKCount, uniqueIndexes, h.undoLog)
	return &tbl, nil
}
