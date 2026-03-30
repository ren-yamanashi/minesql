package statistics

import (
	"minesql/internal/storage/buffer"
	"minesql/internal/storage/catalog"
	"sync"
)

// analyzeThreshold は Analyze 実行判定の閾値 (dirty_count > last_analyze_row_count * threshold)
const analyzeThreshold = 0.1

// tableState はテーブルごとの Analyze 判定に必要な状態
type tableState struct {
	dirtyCount          uint64           // DML で変更されたレコード数の累計
	lastAnalyzeRowCount uint64           // 直近の Analyze 実行時のレコード数
	cachedStats         *TableStatistics // キャッシュ済みの統計情報 (nil なら未取得)
}

// Manager は統計情報の収集とステート管理を行うシングルトン
type Manager struct {
	bufferPool *buffer.BufferPool
	mu         sync.Mutex
	states     map[string]*tableState // テーブル名 -> 状態
}

var (
	mgr  *Manager
	once sync.Once
)

// Init はグローバルな Manager を初期化する
func Init(bp *buffer.BufferPool) *Manager {
	once.Do(func() {
		mgr = &Manager{
			bufferPool: bp,
			states:     make(map[string]*tableState),
		}
	})
	return mgr
}

// Get はグローバルな Manager を取得する
func Get() *Manager {
	if mgr == nil {
		panic("statistics not initialized. call statistics.Init() first")
	}
	return mgr
}

// Reset はグローバルな Manager の状態をリセットする
func Reset() {
	mgr = nil
	once = sync.Once{}
}

// IncrementDirtyCount は DML 実行時にテーブルの dirty_count を加算する
//
// tableName: 対象テーブル名
//
// count: 変更されたレコード数
func (m *Manager) IncrementDirtyCount(tableName string, count uint64) {
	m.mu.Lock()
	defer m.mu.Unlock()

	state := m.getOrCreateState(tableName)
	state.dirtyCount += count
}

// GetOrAnalyze はテーブルの統計情報を返す
// dirty_count が閾値を超えている場合、またはキャッシュがない場合は Analyze を実行する
//
// meta: 対象テーブルのメタデータ
func (m *Manager) GetOrAnalyze(meta *catalog.TableMetadata) (TableStatistics, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	state := m.getOrCreateState(meta.Name)

	if state.cachedStats != nil && !m.shouldAnalyze(state) {
		return *state.cachedStats, nil
	}

	// Analyze を実行
	stats := NewStatistics(meta, m.bufferPool)
	result, err := stats.Analyze()
	if err != nil {
		return TableStatistics{}, err
	}

	// ステートを更新
	state.cachedStats = &result
	state.lastAnalyzeRowCount = result.RecordCount
	state.dirtyCount = 0

	return result, nil
}

// shouldAnalyze は dirty_count が閾値を超えているかを判定する
func (m *Manager) shouldAnalyze(state *tableState) bool {
	threshold := float64(state.lastAnalyzeRowCount) * analyzeThreshold
	return float64(state.dirtyCount) > threshold
}

// getOrCreateState はテーブルの状態を取得する (存在しなければ作成する)
func (m *Manager) getOrCreateState(tableName string) *tableState {
	state, ok := m.states[tableName]
	if !ok {
		state = &tableState{}
		m.states[tableName] = state
	}
	return state
}
