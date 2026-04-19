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

func TestCalcReadTimeForSecondaryIndex(t *testing.T) {
	t.Run("cost.md の式と一致する", func(t *testing.T) {
		// GIVEN: nRanges=1, foundRecords=500, pageReadCost=1.0
		// WHEN
		readTime := calcReadTimeForSecondaryIndex(1, 500, 1.0)

		// THEN: (1 + 500) × 1.0 = 501
		assert.Equal(t, 501.0, readTime)
	})

	t.Run("pageReadCost がキャッシュヒット率を反映する", func(t *testing.T) {
		// GIVEN: 50% キャッシュ → pageReadCost = 0.625
		pageReadCost := 0.5*0.25 + 0.5*1.0 // = 0.625

		// WHEN
		readTime := calcReadTimeForSecondaryIndex(1, 100, pageReadCost)

		// THEN: (1 + 100) × 0.625 = 63.125
		assert.Equal(t, 63.125, readTime)
	})
}

func TestCalcReadTimeForIndexOnly(t *testing.T) {
	t.Run("foundRecords が keysPerBlock 未満の場合は 1 ページ", func(t *testing.T) {
		// GIVEN: keysPerBlock=100, foundRecords=50
		// WHEN
		readTime := calcReadTimeForIndexOnly(50, 100, 1.0)

		// THEN: (50 + 100 - 1) / 100 = 149 / 100 = 1.49
		assert.Equal(t, 1.49, readTime)
	})

	t.Run("foundRecords が keysPerBlock より大きい場合は複数ページ", func(t *testing.T) {
		// GIVEN: keysPerBlock=100, foundRecords=250
		// WHEN
		readTime := calcReadTimeForIndexOnly(250, 100, 1.0)

		// THEN: (250 + 100 - 1) / 100 = 349 / 100 = 3.49
		assert.Equal(t, 3.49, readTime)
	})

	t.Run("keysPerBlock が 0 以下の場合は 1 に補正される", func(t *testing.T) {
		// GIVEN: keysPerBlock=0
		// WHEN
		readTime := calcReadTimeForIndexOnly(10, 0, 1.0)

		// THEN: keysPerBlock=1 に補正 → (10 + 1 - 1) / 1 = 10
		assert.Equal(t, 10.0, readTime)
	})

	t.Run("pageReadCost が反映される", func(t *testing.T) {
		// GIVEN: keysPerBlock=100, foundRecords=100, pageReadCost=0.25 (全キャッシュ)
		// WHEN
		readTime := calcReadTimeForIndexOnly(100, 100, 0.25)

		// THEN: (100 + 100 - 1) / 100 × 0.25 = 1.99 × 0.25 = 0.4975
		assert.Equal(t, 0.4975, readTime)
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

	t.Run("foundRecords が 2 の境界値", func(t *testing.T) {
		// GIVEN & WHEN
		readTime := calcReadTimeForClusteredIndex(1, 2, 10000, 100, 1.0)

		// THEN: foundRecords <= 2 → foundRecords × pageReadCost = 2.0
		assert.Equal(t, 2.0, readTime)
	})

	t.Run("foundRecords が 3 の境界値", func(t *testing.T) {
		// GIVEN & WHEN: 3 > 2 なので比率計算
		readTime := calcReadTimeForClusteredIndex(1, 3, 10000, 100, 1.0)

		// THEN: (1 + (3/10000) × 100) × 1.0 = (1 + 0.03) × 1.0 = 1.03
		assert.InDelta(t, 1.03, readTime, 1e-10)
	})

	t.Run("pageReadCost が反映される", func(t *testing.T) {
		// GIVEN: pageReadCost=0.5
		// WHEN
		readTime := calcReadTimeForClusteredIndex(1, 500, 10000, 100, 0.5)

		// THEN: (1 + (500/10000) × 100) × 0.5 = (1 + 5) × 0.5 = 3.0
		assert.Equal(t, 3.0, readTime)
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
