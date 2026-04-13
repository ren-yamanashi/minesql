package dictionary

import (
	"bytes"
	"fmt"
	"minesql/internal/storage/access"
	"minesql/internal/storage/buffer"
	"sync"
)

// analyzeThreshold は Analyze 実行判定の閾値
const analyzeThreshold = 0.1

// tableState はテーブルごとの Analyze 判定に必要な状態
type tableState struct {
	dirtyCount          uint64      // DML で変更されたレコード数の累計
	lastAnalyzeRowCount uint64      // 直近の Analyze 実行時のレコード数
	cachedStats         *TableStats // キャッシュ済みの統計情報 (nil なら未取得)
}

// StatsCollector はテーブルの統計情報の収集とキャッシュ管理を行う
type StatsCollector struct {
	bufferPool *buffer.BufferPool
	mu         sync.Mutex
	states     map[string]*tableState // テーブル名 -> 状態
}

// NewStatsCollector は StatsCollector を生成する
func NewStatsCollector(bp *buffer.BufferPool) *StatsCollector {
	return &StatsCollector{
		bufferPool: bp,
		states:     make(map[string]*tableState),
	}
}

// Analyze はテーブルの統計情報を収集する
func (sc *StatsCollector) Analyze(meta *TableMeta) (*TableStats, error) {
	// テーブルを構築
	tbl, err := buildTable(meta)
	if err != nil {
		return nil, err
	}

	// 統計情報の収集では MVCC の可視性判定を行わず全レコードを対象にする
	rv := access.NewReadView(0, nil, ^uint64(0))
	vr := access.NewVersionReader(nil)
	iter, err := tbl.Search(sc.bufferPool, rv, vr, access.RecordSearchModeStart{})
	if err != nil {
		return nil, err
	}

	var recordCount uint64
	columnStats := make(map[string]ColumnStats)
	uniqueValues := make(map[string]map[string]struct{}) // カラム名 -> 値の Set
	colMetaList := meta.GetSortedCols()

	// テーブルを直接スキャンして統計情報を収集
	for {
		record, ok, err := iter.Next()
		if err != nil {
			return nil, err
		}
		if !ok {
			break
		}
		recordCount++
		updateColumnStats(colMetaList, record, recordCount, columnStats, uniqueValues)
	}

	// uniqueValues の Set のサイズから V(T, F) を算出
	for colName, values := range uniqueValues {
		colStat := columnStats[colName]
		colStat.UniqueValues = uint64(len(values))
		columnStats[colName] = colStat
	}

	// テーブルのリーフページ数を取得
	leafPageCount, err := tbl.LeafPageCount(sc.bufferPool)
	if err != nil {
		return nil, err
	}

	// プライマリキー B+Tree の高さを取得
	primaryHeight, err := tbl.Height(sc.bufferPool)
	if err != nil {
		return nil, err
	}

	// ユニークインデックスの統計情報を収集
	secondaryIndexStats, err := sc.analyzeIndex(tbl)
	if err != nil {
		return nil, err
	}

	return &TableStats{
		RecordCount:   recordCount,
		LeafPageCount: leafPageCount,
		TreeHeight:    primaryHeight,
		ColStats:      columnStats,
		IdxStats:      secondaryIndexStats,
	}, nil
}

// GetOrAnalyze はテーブルの統計情報を返す
// dirtyCount が閾値を超えている場合、またはキャッシュがない場合は Analyze を実行する
func (sc *StatsCollector) GetOrAnalyze(meta *TableMeta) (*TableStats, error) {
	sc.mu.Lock()
	defer sc.mu.Unlock()

	state := sc.getOrCreateState(meta.Name)

	if state.cachedStats != nil && !sc.shouldAnalyze(state) {
		return state.cachedStats, nil
	}

	result, err := sc.Analyze(meta)
	if err != nil {
		return nil, err
	}

	state.cachedStats = result
	state.lastAnalyzeRowCount = result.RecordCount
	state.dirtyCount = 0

	return result, nil
}

// IncrementDirtyCount は DML 実行時にテーブルの dirtyCount を加算する
func (sc *StatsCollector) IncrementDirtyCount(tableName string, count uint64) {
	sc.mu.Lock()
	defer sc.mu.Unlock()

	state := sc.getOrCreateState(tableName)
	state.dirtyCount += count
}

// shouldAnalyze は Analyze を実行すべきかを判定する
func (sc *StatsCollector) shouldAnalyze(state *tableState) bool {
	threshold := float64(state.lastAnalyzeRowCount) * analyzeThreshold
	return float64(state.dirtyCount) > threshold
}

// getOrCreateState はテーブル名に対応する tableState を取得する
//
// テーブル名に対応する tableState が存在しない場合は新規作成して返す
func (sc *StatsCollector) getOrCreateState(tableName string) *tableState {
	state, ok := sc.states[tableName]
	if !ok {
		state = &tableState{}
		sc.states[tableName] = state
	}
	return state
}

// updateColumnStats は 1 レコード分の各カラムの統計情報 (uniqueValues, min, max) を更新する
func updateColumnStats(
	colMetaList []*ColumnMeta,
	record [][]byte,
	recordCount uint64,
	columnStats map[string]ColumnStats,
	uniqueValues map[string]map[string]struct{},
) {
	for i, colMeta := range colMetaList {
		colName := colMeta.Name
		colValue := record[i]
		colStat := columnStats[colName]

		if uniqueValues[colName] == nil {
			uniqueValues[colName] = make(map[string]struct{})
		}
		uniqueValues[colName][string(colValue)] = struct{}{}

		if recordCount == 1 {
			colStat.MinValue = colValue
			colStat.MaxValue = colValue
		} else {
			if bytes.Compare(colValue, colStat.MinValue) < 0 {
				colStat.MinValue = colValue
			}
			if bytes.Compare(colValue, colStat.MaxValue) > 0 {
				colStat.MaxValue = colValue
			}
		}

		columnStats[colName] = colStat
	}
}

// buildTable は TableMeta から Table を構築する (UndoManager なし)
func buildTable(meta *TableMeta) (*access.Table, error) {
	var uniqueIndexes []*access.UniqueIndex
	for _, idxMeta := range meta.Indexes {
		if idxMeta.Type == IndexTypeUnique {
			colMeta, ok := meta.GetColByName(idxMeta.ColName)
			if !ok {
				return nil, fmt.Errorf("column %s not found in table %s", idxMeta.ColName, meta.Name)
			}
			ui := access.NewUniqueIndex(idxMeta.Name, idxMeta.ColName, idxMeta.DataMetaPageId, colMeta.Pos, meta.PKCount)
			uniqueIndexes = append(uniqueIndexes, ui)
		}
	}
	tbl := access.NewTable(meta.Name, meta.DataMetaPageId, meta.PKCount, uniqueIndexes, nil, nil)
	return &tbl, nil
}

// analyzeIndex はテーブルのユニークインデックスの統計情報を収集する
func (sc *StatsCollector) analyzeIndex(tbl *access.Table) (map[string]IndexStats, error) {
	idxStats := make(map[string]IndexStats)
	for _, uIdx := range tbl.UniqueIndexes {
		height, err := uIdx.Height(sc.bufferPool)
		if err != nil {
			return nil, err
		}
		leafPageCount, err := uIdx.LeafPageCount(sc.bufferPool)
		if err != nil {
			return nil, err
		}
		idxStats[uIdx.Name] = IndexStats{
			Height:        height,
			LeafPageCount: leafPageCount,
		}
	}
	return idxStats, nil
}
