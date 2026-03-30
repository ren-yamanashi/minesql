package engine

import "minesql/internal/storage/statistics"

// AnalyzeTable はテーブルの統計情報を収集して返す
func (e *Engine) AnalyzeTable(meta *TableMetadata) (TableStatistics, error) {
	st := statistics.NewStatistics(meta, e.BufferPool)
	return st.Analyze()
}
