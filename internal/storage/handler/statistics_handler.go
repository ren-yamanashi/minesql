package handler

import "minesql/internal/storage/dictionary"

// AnalyzeTable はテーブルの統計情報を収集して返す
func (h *Handler) AnalyzeTable(meta *TableMetadata) (TableStatistics, error) {
	st := dictionary.NewStatistics(meta, h.BufferPool)
	return st.Analyze()
}
