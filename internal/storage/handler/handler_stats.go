package handler

// AnalyzeTable はテーブルの統計情報を収集して返す
func (h *Handler) AnalyzeTable(meta *TableMetadata) (*TableStatistics, error) {
	return h.StatsCollector.Analyze(meta)
}
