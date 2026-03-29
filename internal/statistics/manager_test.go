package statistics

import (
	"minesql/internal/engine"
	"minesql/internal/executor"
	"minesql/internal/undo"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestGetOrAnalyze(t *testing.T) {
	t.Run("初回呼び出しで Analyze が実行されキャッシュされる", func(t *testing.T) {
		// GIVEN: 3 レコード挿入済み
		setupStatisticsTable(t)
		defer engine.Reset()
		defer Reset()

		eng := engine.Get()
		Init(eng.BufferPool)
		m := Get()

		meta, ok := eng.Catalog.GetTableMetadataByName("products")
		assert.True(t, ok)

		// WHEN: 初回 GetOrAnalyze
		result, err := m.GetOrAnalyze(meta)

		// THEN: Analyze が実行され統計値が返る
		assert.NoError(t, err)
		assert.Equal(t, uint64(3), result.RecordCount)
	})

	t.Run("dirty_count が閾値以下ならキャッシュが返る", func(t *testing.T) {
		// GIVEN: 3 レコードで GetOrAnalyze 済み
		setupStatisticsTable(t)
		defer engine.Reset()
		defer Reset()

		eng := engine.Get()
		Init(eng.BufferPool)
		m := Get()

		meta, ok := eng.Catalog.GetTableMetadataByName("products")
		assert.True(t, ok)

		_, err := m.GetOrAnalyze(meta)
		assert.NoError(t, err)

		// WHEN: dirty_count=0 のまま GetOrAnalyze を呼ぶ
		result, err := m.GetOrAnalyze(meta)

		// THEN: キャッシュされた統計値が返る (RecordCount は 3 のまま)
		assert.NoError(t, err)
		assert.Equal(t, uint64(3), result.RecordCount)
	})

	t.Run("dirty_count が閾値を超えると再 Analyze が実行される", func(t *testing.T) {
		// GIVEN: 3 レコードで GetOrAnalyze 済み
		setupStatisticsTable(t)
		defer engine.Reset()
		defer Reset()

		eng := engine.Get()
		Init(eng.BufferPool)
		m := Get()

		meta, ok := eng.Catalog.GetTableMetadataByName("products")
		assert.True(t, ok)

		_, err := m.GetOrAnalyze(meta)
		assert.NoError(t, err)

		// WHEN: 1 レコード追加して dirty_count を加算 (閾値 = 3 * 0.1 = 0.3, dirty_count = 1 > 0.3)
		undoLog := undo.NewUndoLog()
		var trxId undo.TrxId = 1
		insertRecords(t, undoLog, trxId, "products",
			[]executor.Record{
				{[]byte("4"), []byte("Donut"), []byte("Snack")},
			},
		)
		m.IncrementDirtyCount("products", 1)

		result, err := m.GetOrAnalyze(meta)

		// THEN: 再 Analyze が実行され、最新の統計値が返る
		assert.NoError(t, err)
		assert.Equal(t, uint64(4), result.RecordCount)
	})

	t.Run("再 Analyze 後に dirty_count がリセットされる", func(t *testing.T) {
		// GIVEN: 3 レコードで GetOrAnalyze 済み → dirty_count 加算 → 再 Analyze 済み
		setupStatisticsTable(t)
		defer engine.Reset()
		defer Reset()

		eng := engine.Get()
		Init(eng.BufferPool)
		m := Get()

		meta, ok := eng.Catalog.GetTableMetadataByName("products")
		assert.True(t, ok)

		_, err := m.GetOrAnalyze(meta)
		assert.NoError(t, err)

		// 1 レコード追加して再 Analyze を発火させる
		undoLog := undo.NewUndoLog()
		var trxId undo.TrxId = 1
		insertRecords(t, undoLog, trxId, "products",
			[]executor.Record{
				{[]byte("4"), []byte("Donut"), []byte("Snack")},
			},
		)
		m.IncrementDirtyCount("products", 1)
		_, err = m.GetOrAnalyze(meta)
		assert.NoError(t, err)

		// WHEN: さらに 1 レコード追加するが、dirty_count はリセット済みなので
		// 再 Analyze は走らず、キャッシュから RecordCount=4 が返る
		insertRecords(t, undoLog, trxId, "products",
			[]executor.Record{
				{[]byte("5"), []byte("Egg"), []byte("Dairy")},
			},
		)
		// dirty_count を加算しない → 閾値以下のまま

		result, err := m.GetOrAnalyze(meta)

		// THEN: キャッシュが返る (RecordCount は再 Analyze 時の 4 のまま)
		assert.NoError(t, err)
		assert.Equal(t, uint64(4), result.RecordCount)
	})
}
