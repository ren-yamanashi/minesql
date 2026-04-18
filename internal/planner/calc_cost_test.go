package planner

import (
	"minesql/internal/storage/btree"
	"minesql/internal/storage/handler"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestCalcTableScanCost(t *testing.T) {
	t.Run("テーブルスキャンのコストが統計値と一致する", func(t *testing.T) {
		// GIVEN
		stats := newTestStats()

		// WHEN
		cost := calcTableScanCost(stats)

		// THEN: B(s) = B(T), R(s) = R(T), V(s,F) = V(T,F)
		assert.Equal(t, float64(50), cost.DiskAccesses)
		assert.Equal(t, float64(1000), cost.RecordCount)
		assert.Equal(t, float64(1000), cost.UniqueValues["id"])
		assert.Equal(t, float64(10), cost.UniqueValues["category"])
	})
}

func TestCalcSelectEqualCost(t *testing.T) {
	t.Run("等価比較で R(s) が V(s1,A) で除算される", func(t *testing.T) {
		// GIVEN: テーブルスキャンを下位スキャンとする
		inner := calcTableScanCost(newTestStats())

		// WHEN: WHERE category = 'Fruit'
		cost := calcSelectEqualCost(inner, "category")

		// THEN: R(s) = 1000 / 10 = 100, V(s, category) = 1
		assert.Equal(t, float64(50), cost.DiskAccesses) // B(s) = B(s1)
		assert.Equal(t, float64(100), cost.RecordCount)
		assert.Equal(t, float64(1), cost.UniqueValues["category"])
		assert.Equal(t, float64(1000), cost.UniqueValues["id"]) // 他カラムは変化なし
	})
}

func TestCalcSelectNotEqualCost(t *testing.T) {
	t.Run("非等価比較で R(s) が (V-1)/V 倍になる", func(t *testing.T) {
		// GIVEN
		inner := calcTableScanCost(newTestStats())

		// WHEN: WHERE category != 'Fruit'
		cost := calcSelectNotEqualCost(inner, "category")

		// THEN: R(s) = 1000 * (10-1)/10 = 900, V(s, category) = 9
		assert.Equal(t, float64(50), cost.DiskAccesses)
		assert.Equal(t, float64(900), cost.RecordCount)
		assert.Equal(t, float64(9), cost.UniqueValues["category"])
		assert.Equal(t, float64(1000), cost.UniqueValues["id"])
	})
}

func TestCalcSelectRangeCost(t *testing.T) {
	t.Run("範囲比較で選択率に応じたコストが算出される", func(t *testing.T) {
		// GIVEN: selectivity = 0.3 (30%)
		inner := calcTableScanCost(newTestStats())

		// WHEN: WHERE category > 'Cat7' (selectivity = 0.3)
		cost := calcSelectRangeCost(inner, "category", 0.3)

		// THEN: R(s) = 1000 * 0.3 = 300, V(s, category) = 10 * 0.3 = 3
		assert.Equal(t, float64(50), cost.DiskAccesses)
		assert.Equal(t, float64(300), cost.RecordCount)
		assert.Equal(t, float64(3), cost.UniqueValues["category"])
		assert.Equal(t, float64(1000), cost.UniqueValues["id"])
	})
}

func TestCalcPKSelectEqualCost(t *testing.T) {
	t.Run("PK 等価比較で B(s) が H(T) になる", func(t *testing.T) {
		// GIVEN: テーブルスキャンを下位スキャンとする (H(T) = 4)
		stats := newTestStats()
		inner := calcTableScanCost(stats)

		// WHEN: WHERE id = 42
		cost := calcPKSelectEqualCost(inner, "id", stats.TreeHeight)

		// THEN: B(s) = H(T) = 4, R(s) = 1000/1000 = 1, V(s, id) = 1
		assert.Equal(t, float64(4), cost.DiskAccesses)
		assert.Equal(t, float64(1), cost.RecordCount)
		assert.Equal(t, float64(1), cost.UniqueValues["id"])
		assert.Equal(t, float64(10), cost.UniqueValues["category"]) // 他カラムは変化なし
	})
}

func TestCalcPKSelectRangeGTCost(t *testing.T) {
	t.Run("PK 範囲比較 (>) で B(s) が H(T) + sel * B(T) になる", func(t *testing.T) {
		// GIVEN: H(T) = 4, B(T) = 50, selectivity = 0.3
		stats := newTestStats()
		inner := calcTableScanCost(stats)

		// WHEN: WHERE id > 700
		cost := calcPKSelectRangeGTCost(inner, "id", 0.3, stats.TreeHeight)

		// THEN: B(s) = 4 + 0.3 * 50 = 19, R(s) = 1000 * 0.3 = 300
		assert.Equal(t, float64(19), cost.DiskAccesses)
		assert.Equal(t, float64(300), cost.RecordCount)
		assert.Equal(t, float64(300), cost.UniqueValues["id"]) // 1000 * 0.3
		assert.Equal(t, float64(10), cost.UniqueValues["category"])
	})
}

func TestCalcPKSelectRangeLTCost(t *testing.T) {
	t.Run("PK 範囲比較 (<) で B(s) が sel * B(T) になる", func(t *testing.T) {
		// GIVEN: B(T) = 50, selectivity = 0.3
		stats := newTestStats()
		inner := calcTableScanCost(stats)

		// WHEN: WHERE id < 300
		cost := calcPKSelectRangeLTCost(inner, "id", 0.3)

		// THEN: B(s) = 0.3 * 50 = 15, R(s) = 1000 * 0.3 = 300
		assert.Equal(t, float64(15), cost.DiskAccesses)
		assert.Equal(t, float64(300), cost.RecordCount)
		assert.Equal(t, float64(300), cost.UniqueValues["id"]) // 1000 * 0.3
		assert.Equal(t, float64(10), cost.UniqueValues["category"])
	})
}

func TestCalcProjectCost(t *testing.T) {
	t.Run("射影スキャンのコストが下位スキャンと同一", func(t *testing.T) {
		// GIVEN
		inner := calcTableScanCost(newTestStats())

		// WHEN
		cost := calcProjectCost(inner)

		// THEN: すべて下位スキャンと同じ
		assert.Equal(t, inner.DiskAccesses, cost.DiskAccesses)
		assert.Equal(t, inner.RecordCount, cost.RecordCount)
		assert.Equal(t, inner.UniqueValues["id"], cost.UniqueValues["id"])
	})
}

func TestCalcIndexScanCost(t *testing.T) {
	t.Run("インデックススキャンの B(s) が H(I) になる", func(t *testing.T) {
		// GIVEN
		stats := newTestStats()

		// WHEN
		cost := calcIndexScanCost(stats, 3)

		// THEN: B(s) = H(I) = 3, R(s) = R(T), V(s,F) = V(T,F)
		assert.Equal(t, float64(3), cost.DiskAccesses)
		assert.Equal(t, float64(1000), cost.RecordCount)
		assert.Equal(t, float64(1000), cost.UniqueValues["name"])
	})
}

func TestCalcIndexSelectEqualCost(t *testing.T) {
	t.Run("インデックス等価比較のコストが正しく算出される", func(t *testing.T) {
		// GIVEN
		stats := newTestStats()

		// WHEN: ユニークインデックス name で等価比較
		cost := calcIndexSelectEqualCost(stats, "name", 3)

		// THEN: B(s) = H(I) = 3, R(s) = 1000/1000 = 1, V(s, name) = 1
		assert.Equal(t, float64(3), cost.DiskAccesses)
		assert.Equal(t, float64(1), cost.RecordCount)
		assert.Equal(t, float64(1), cost.UniqueValues["name"])
		assert.Equal(t, float64(10), cost.UniqueValues["category"])
	})
}

func TestCalcIndexSelectNotEqualCost(t *testing.T) {
	t.Run("インデックス非等価比較のコストが正しく算出される", func(t *testing.T) {
		// GIVEN: indexLeafPages = 20
		stats := newTestStats()

		// WHEN: WHERE category != 'Fruit'
		cost := calcIndexSelectNotEqualCost(stats, "category", 3, 20)

		// THEN: B(s) = H(I) + Bl(I) = 3 + 20 = 23
		//       R(s) = 1000 * (10-1)/10 = 900
		//       V(s, category) = 10 - 1 = 9
		assert.Equal(t, float64(23), cost.DiskAccesses)
		assert.Equal(t, float64(900), cost.RecordCount)
		assert.Equal(t, float64(9), cost.UniqueValues["category"])
		assert.Equal(t, float64(1000), cost.UniqueValues["id"])
	})
}

func TestCalcIndexSelectRangeCost(t *testing.T) {
	t.Run("インデックス範囲比較のコストが正しく算出される", func(t *testing.T) {
		// GIVEN: indexLeafPages = 20, selectivity = 0.3
		stats := newTestStats()

		// WHEN
		cost := calcIndexSelectRangeCost(stats, "name", 3, 20, 0.3)

		// THEN: B(s) = 3 + 20*0.3 = 9, R(s) = 1000*0.3 = 300
		assert.Equal(t, float64(9), cost.DiskAccesses)
		assert.Equal(t, float64(300), cost.RecordCount)
		assert.Equal(t, float64(300), cost.UniqueValues["name"]) // 1000 * 0.3
		assert.Equal(t, float64(10), cost.UniqueValues["category"])
	})
}

func TestCalcIndexTableEqualCost(t *testing.T) {
	t.Run("インデックス+テーブル等価比較のコストが正しく算出される", func(t *testing.T) {
		// GIVEN: primaryHeight = 4
		stats := newTestStats()

		// WHEN: ユニークインデックス name で等価比較
		cost := calcIndexTableEqualCost(stats, "name", 3, 4)

		// THEN: R(s) = 1, B(s) = H(I) + R(s)*H(T) = 3 + 1*4 = 7
		assert.Equal(t, float64(7), cost.DiskAccesses)
		assert.Equal(t, float64(1), cost.RecordCount)
		assert.Equal(t, float64(1), cost.UniqueValues["name"])
	})
}

func TestCalcIndexTableNotEqualCost(t *testing.T) {
	t.Run("インデックス+テーブル非等価比較のコストが正しく算出される", func(t *testing.T) {
		// GIVEN: indexLeafPages = 20, primaryHeight = 4
		stats := newTestStats()

		// WHEN: WHERE category != 'Fruit'
		cost := calcIndexTableNotEqualCost(stats, "category", 3, 20, 4)

		// THEN: R(s) = 1000 * 9/10 = 900
		//       B(s) = H(I) + Bl(I) + R(s)*H(T) = 3 + 20 + 900*4 = 3623
		assert.Equal(t, float64(3623), cost.DiskAccesses)
		assert.Equal(t, float64(900), cost.RecordCount)
		assert.Equal(t, float64(9), cost.UniqueValues["category"])
		assert.Equal(t, float64(1000), cost.UniqueValues["id"])
	})
}

func TestCalcIndexTableRangeCost(t *testing.T) {
	t.Run("インデックス+テーブル範囲比較のコストが正しく算出される", func(t *testing.T) {
		// GIVEN: indexLeafPages = 20, selectivity = 0.3, primaryHeight = 4
		stats := newTestStats()

		// WHEN
		cost := calcIndexTableRangeCost(stats, "name", 3, 20, 0.3, 4)

		// THEN: R(s) = 300, B(s) = 3 + 20*0.3 + 300*4 = 3 + 6 + 1200 = 1209
		assert.Equal(t, float64(1209), cost.DiskAccesses)
		assert.Equal(t, float64(300), cost.RecordCount)
		assert.Equal(t, float64(300), cost.UniqueValues["name"])
	})
}

func TestCalcRangeSelectivity(t *testing.T) {
	t.Run("> 演算子で選択率が正しく算出される", func(t *testing.T) {
		// GIVEN: min=1, max=100, c=70
		// WHEN
		sel := calcRangeSelectivity(">", 70, 1, 100)

		// THEN: (100-70)/(100-1) ≒ 0.3030
		assert.InDelta(t, 30.0/99.0, sel, 0.0001)
	})

	t.Run("< 演算子で選択率が正しく算出される", func(t *testing.T) {
		// GIVEN: min=1, max=100, c=30
		// WHEN
		sel := calcRangeSelectivity("<", 30, 1, 100)

		// THEN: (30-1)/(100-1) ≒ 0.2929
		assert.InDelta(t, 29.0/99.0, sel, 0.0001)
	})

	t.Run("min と max が等しい場合はデフォルト選択率を返す", func(t *testing.T) {
		// GIVEN: min=max=50
		// WHEN
		sel := calcRangeSelectivity(">", 30, 50, 50)

		// THEN: 1/3
		assert.InDelta(t, 1.0/3.0, sel, 0.0001)
	})

	t.Run("選択率が 0〜1 にクランプされる", func(t *testing.T) {
		// GIVEN: c が max を超えている
		// WHEN
		sel := calcRangeSelectivity(">", 200, 1, 100)

		// THEN: 0 にクランプ
		assert.Equal(t, float64(0), sel)
	})
}

func TestTotalCost(t *testing.T) {
	t.Run("I/O コストと CPU コストが重み付けされる", func(t *testing.T) {
		// GIVEN: B(s) = 50, R(s) = 1000
		cost := ScanCost{
			DiskAccesses: 50,
			RecordCount:  1000,
		}

		// WHEN
		total := cost.TotalCost()

		// THEN: 50 * 1.0 + 1000 * 0.1 = 150
		assert.Equal(t, float64(150), total)
	})

	t.Run("R(s) が小さい場合は B(s) が支配的になる", func(t *testing.T) {
		// GIVEN: B(s) = 50, R(s) = 1
		cost := ScanCost{
			DiskAccesses: 50,
			RecordCount:  1,
		}

		// WHEN
		total := cost.TotalCost()

		// THEN: 50 * 1.0 + 1 * 0.1 = 50.1
		assert.Equal(t, float64(50.1), total)
	})

	t.Run("B(s) が小さく R(s) が大きい場合は CPU コストが影響する", func(t *testing.T) {
		// GIVEN: B(s) = 4, R(s) = 10000
		cost := ScanCost{
			DiskAccesses: 4,
			RecordCount:  10000,
		}

		// WHEN
		total := cost.TotalCost()

		// THEN: 4 * 1.0 + 10000 * 0.1 = 1004
		assert.Equal(t, float64(1004), total)
	})
}

// -----------------------------------------------
// 新コストモデルのテスト (cost2.md 準拠)
// -----------------------------------------------

func TestCalcFullScanCost(t *testing.T) {
	t.Run("cost2.md の計算例と一致する", func(t *testing.T) {
		// GIVEN: cost2.md の例
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
		assert.Equal(t, 100.0, readCost)                          // I/O のみ
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
	t.Run("cost2.md のセカンダリインデックス計算例と一致する", func(t *testing.T) {
		// GIVEN: cost2.md の例
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
		// GIVEN
		// WHEN
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

// newTestStats はテスト用の TableStatistics を生成する
//
// products テーブル: R(T) = 1000, B(T) = 50, H(T) = 4
//
//   - id: V = 1000, min = "1", max = "1000"
//   - name: V = 1000, min = "A", max = "Z"
//   - category: V = 10, min = "Cat1", max = "Cat10"
//
// セカンダリインデックス: name (H = 3)
func newTestStats() *handler.TableStatistics {
	return &handler.TableStatistics{
		RecordCount:   1000,
		LeafPageCount: 50,
		TreeHeight:    4,
		ColStats: map[string]handler.ColumnStatistics{
			"id":       {UniqueValues: 1000, MinValue: []byte("1"), MaxValue: []byte("1000")},
			"name":     {UniqueValues: 1000, MinValue: []byte("A"), MaxValue: []byte("Z")},
			"category": {UniqueValues: 10, MinValue: []byte("Cat1"), MaxValue: []byte("Cat10")},
		},
		IdxStats: map[string]handler.IndexStatistics{
			"name": {Height: 3},
		},
	}
}
