package planner

import (
	"minesql/internal/storage/btree"
	"minesql/internal/storage/handler"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestCalcFullScanCost(t *testing.T) {
	t.Run("cost.md の計算例と一致する", func(t *testing.T) {
		// GIVEN: cost.md の例
		// RecordCount=74822, LeafPageCount=1924 (= 7880704 / 4096), pageReadCost=1.0
		stats := &handler.TableStatistics{
			RecordCount:   74822,
			LeafPageCount: 1924,
		}

		// WHEN
		cost := calcFullScanCost(stats, 1.0)

		// THEN: 1924 × 1.0 + 74822 × 0.1 = 9406.2
		assert.Equal(t, 9406.2, cost)
	})

	t.Run("readCost と最終コストが分離されている", func(t *testing.T) {
		// GIVEN
		stats := &handler.TableStatistics{
			RecordCount:   1000,
			LeafPageCount: 100,
		}

		// WHEN
		readCost := calcFullScanReadCost(stats, 1.0)
		fullCost := calcFullScanCost(stats, 1.0)

		// THEN
		assert.Equal(t, 100.0, readCost)                      // I/O のみ
		assert.Equal(t, 100.0+1000*RowEvaluateCost, fullCost) // I/O + CPU
	})
}

func TestCalcUniqueScanCost(t *testing.T) {
	t.Run("コスト 1.0 固定", func(t *testing.T) {
		// WHEN
		cost := calcUniqueScanCost()

		// THEN
		assert.Equal(t, 1.0, cost)
	})
}

func TestCalcRangeScanCost(t *testing.T) {
	t.Run("cost.md のセカンダリインデックス計算例と一致する", func(t *testing.T) {
		// GIVEN: cost.md の例
		// foundRecords=500, nRanges=1, pageReadCost=1.0
		foundRecords := 500.0
		readTime := calcReadTimeForSecondaryIndex(1, foundRecords, 1.0)

		// WHEN
		cost := calcRangeScanCost(readTime, foundRecords)

		// THEN: readTime = (1+500)×1.0 = 501
		//       cost = 501 + 2×500×0.1 + 0.01 = 601.01
		assert.Equal(t, 501.0, readTime)
		assert.Equal(t, 601.01, cost)
	})
}

func TestCalcReadTimeForClusteredIndex(t *testing.T) {
	t.Run("foundRecords が 2 以下の場合", func(t *testing.T) {
		// GIVEN & WHEN
		readTime := calcReadTimeForClusteredIndex(1, 1, 10000, 100, 1.0)

		// THEN: foundRecords × pageReadCost = 1 × 1.0 = 1
		assert.Equal(t, 1.0, readTime)
	})

	t.Run("foundRecords が 3 以上の場合", func(t *testing.T) {
		// GIVEN: foundRecords=500, totalRows=10000, scanTime=100, nRanges=1
		// WHEN
		readTime := calcReadTimeForClusteredIndex(1, 500, 10000, 100, 1.0)

		// THEN: (1 + (500/10000) × 100) × 1.0 = (1 + 5) × 1.0 = 6
		assert.Equal(t, 6.0, readTime)
	})
}

func TestCalcPageReadCost(t *testing.T) {
	t.Run("データ投入直後はリーフがキャッシュに載っておりコストが 1.0 未満になる", func(t *testing.T) {
		// GIVEN: ストレージを初期化し、テーブルにデータを投入
		setupUsersTable(t)
		hdl := handler.Get()
		tbl, err := hdl.GetTable("users")
		assert.NoError(t, err)

		// WHEN
		bt := btree.NewBTree(tbl.MetaPageId)
		cost, err := calcPageReadCost(hdl.BufferPool, bt)

		// THEN: 挿入直後なのでキャッシュに載っている → コストが 1.0 以下
		assert.NoError(t, err)
		assert.LessOrEqual(t, cost, 1.0)
		assert.GreaterOrEqual(t, cost, 0.25) // 全キャッシュなら 0.25
	})
}
