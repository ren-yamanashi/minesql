package btree

import (
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRecordsInRange(t *testing.T) {
	t.Run("同一リーフページ内の範囲を正確にカウントできる", func(t *testing.T) {
		// GIVEN: 少数のレコード (1 ページに収まる)
		bt, bp := setupBTree(t)
		bt.mustInsert(bp, "bbb", "v1")
		bt.mustInsert(bp, "ddd", "v2")
		bt.mustInsert(bp, "fff", "v3")
		bt.mustInsert(bp, "hhh", "v4")

		// WHEN: "bbb" <= key <= "fff" (全境界含む)
		count, err := bt.RecordsInRange(bp, []byte("bbb"), []byte("fff"), true, true)

		// THEN
		require.NoError(t, err)
		assert.Equal(t, int64(3), count) // bbb, ddd, fff
	})

	t.Run("境界を含まない場合のカウントが正しい", func(t *testing.T) {
		// GIVEN
		bt, bp := setupBTree(t)
		bt.mustInsert(bp, "bbb", "v1")
		bt.mustInsert(bp, "ddd", "v2")
		bt.mustInsert(bp, "fff", "v3")
		bt.mustInsert(bp, "hhh", "v4")

		// WHEN: "bbb" < key < "hhh" (境界を含まない)
		count, err := bt.RecordsInRange(bp, []byte("bbb"), []byte("hhh"), false, false)

		// THEN
		require.NoError(t, err)
		assert.Equal(t, int64(2), count) // ddd, fff
	})

	t.Run("存在しないキーで leftIncl=false の場合に GE 位置のレコードが除外されない", func(t *testing.T) {
		// GIVEN
		bt, bp := setupBTree(t)
		bt.mustInsert(bp, "aaa", "v1")
		bt.mustInsert(bp, "bbb", "v2")
		bt.mustInsert(bp, "ccc", "v3")
		bt.mustInsert(bp, "ddd", "v4")

		// WHEN: > "bb0" (bb0 は存在しない、GE 位置は "bbb")
		// "bbb" > "bb0" なので "bbb" は範囲に含まれるべき
		count, err := bt.RecordsInRange(bp, []byte("bb0"), nil, false, true)

		// THEN: bbb, ccc, ddd = 3
		require.NoError(t, err)
		assert.Equal(t, int64(3), count)
	})

	t.Run("存在しないキーで rightIncl=false の場合に LE 位置のレコードが除外されない", func(t *testing.T) {
		// GIVEN
		bt, bp := setupBTree(t)
		bt.mustInsert(bp, "aaa", "v1")
		bt.mustInsert(bp, "bbb", "v2")
		bt.mustInsert(bp, "ccc", "v3")
		bt.mustInsert(bp, "ddd", "v4")

		// WHEN: < "ccd" (ccd は存在しない、SearchSlotNum → 3 (ddd の位置)、LE 調整 → 2 (ccc の位置))
		// "ccc" < "ccd" なので "ccc" は範囲に含まれるべき
		count, err := bt.RecordsInRange(bp, nil, []byte("ccd"), true, false)

		// THEN: aaa, bbb, ccc = 3
		require.NoError(t, err)
		assert.Equal(t, int64(3), count)
	})

	t.Run("存在するキーで leftIncl=false の場合は正しく除外される", func(t *testing.T) {
		// GIVEN
		bt, bp := setupBTree(t)
		bt.mustInsert(bp, "aaa", "v1")
		bt.mustInsert(bp, "bbb", "v2")
		bt.mustInsert(bp, "ccc", "v3")
		bt.mustInsert(bp, "ddd", "v4")

		// WHEN: > "bbb" (bbb は存在する)
		// "bbb" > "bbb" は偽なので "bbb" は除外されるべき
		count, err := bt.RecordsInRange(bp, []byte("bbb"), nil, false, true)

		// THEN: ccc, ddd = 2
		require.NoError(t, err)
		assert.Equal(t, int64(2), count)
	})

	t.Run("存在しないキーでは境界除外が適用されない", func(t *testing.T) {
		// GIVEN
		bt, bp := setupBTree(t)
		bt.mustInsert(bp, "aaa", "v1")
		bt.mustInsert(bp, "bbb", "v2")
		bt.mustInsert(bp, "ccc", "v3")
		bt.mustInsert(bp, "ddd", "v4")

		// WHEN: "aaz" < key < "ccz" (aaz, ccz は存在しない)
		// GE("aaz") → slotNum=1 ("bbb"), found=false
		// LE("ccz") → SearchSlotNum("ccz")=3 ("ddd"), found=false → 調整後 slotNum=2 ("ccc")
		// found=false なので境界除外は適用されず、bbb と ccc が含まれる
		count, err := bt.RecordsInRange(bp, []byte("aaz"), []byte("ccz"), false, false)

		// THEN: bbb, ccc = 2
		require.NoError(t, err)
		assert.Equal(t, int64(2), count)
	})

	t.Run("存在するキーでは境界除外が正しく適用される", func(t *testing.T) {
		// GIVEN
		bt, bp := setupBTree(t)
		bt.mustInsert(bp, "aaa", "v1")
		bt.mustInsert(bp, "bbb", "v2")
		bt.mustInsert(bp, "ccc", "v3")
		bt.mustInsert(bp, "ddd", "v4")

		// WHEN: "aaa" < key < "ddd" (aaa, ddd は存在する)
		// found=true なので境界除外が適用され、aaa と ddd は除外される
		count, err := bt.RecordsInRange(bp, []byte("aaa"), []byte("ddd"), false, false)

		// THEN: bbb, ccc = 2
		require.NoError(t, err)
		assert.Equal(t, int64(2), count)
	})

	t.Run("存在しないキーの範囲でも正しく推定できる", func(t *testing.T) {
		// GIVEN
		bt, bp := setupBTree(t)
		bt.mustInsert(bp, "bbb", "v1")
		bt.mustInsert(bp, "ddd", "v2")
		bt.mustInsert(bp, "fff", "v3")
		bt.mustInsert(bp, "hhh", "v4")

		// WHEN: "ccc" <= key <= "ggg" (ccc, ggg は存在しない)
		count, err := bt.RecordsInRange(bp, []byte("ccc"), []byte("ggg"), true, true)

		// THEN
		require.NoError(t, err)
		assert.Equal(t, int64(2), count) // ddd, fff
	})

	t.Run("nil キーで全範囲を指定できる", func(t *testing.T) {
		// GIVEN
		bt, bp := setupBTree(t)
		bt.mustInsert(bp, "aaa", "v1")
		bt.mustInsert(bp, "bbb", "v2")
		bt.mustInsert(bp, "ccc", "v3")

		// WHEN: nil <= key <= nil (全範囲)
		count, err := bt.RecordsInRange(bp, nil, nil, true, true)

		// THEN
		require.NoError(t, err)
		assert.Equal(t, int64(3), count)
	})

	t.Run("下限のみ nil で先頭からの範囲を指定できる", func(t *testing.T) {
		// GIVEN
		bt, bp := setupBTree(t)
		bt.mustInsert(bp, "aaa", "v1")
		bt.mustInsert(bp, "bbb", "v2")
		bt.mustInsert(bp, "ccc", "v3")

		// WHEN: nil <= key <= "bbb"
		count, err := bt.RecordsInRange(bp, nil, []byte("bbb"), true, true)

		// THEN
		require.NoError(t, err)
		assert.Equal(t, int64(2), count) // aaa, bbb
	})

	t.Run("上限のみ nil で末尾までの範囲を指定できる", func(t *testing.T) {
		// GIVEN
		bt, bp := setupBTree(t)
		bt.mustInsert(bp, "aaa", "v1")
		bt.mustInsert(bp, "bbb", "v2")
		bt.mustInsert(bp, "ccc", "v3")

		// WHEN: "bbb" <= key <= nil
		count, err := bt.RecordsInRange(bp, []byte("bbb"), nil, true, true)

		// THEN
		require.NoError(t, err)
		assert.Equal(t, int64(2), count) // bbb, ccc
	})

	t.Run("空のツリーで 0 を返す", func(t *testing.T) {
		// GIVEN
		bt, bp := setupBTree(t)

		// WHEN
		count, err := bt.RecordsInRange(bp, []byte("aaa"), []byte("zzz"), true, true)

		// THEN
		require.NoError(t, err)
		assert.Equal(t, int64(0), count)
	})

	t.Run("範囲外のキーを指定すると 0 を返す", func(t *testing.T) {
		// GIVEN
		bt, bp := setupBTree(t)
		bt.mustInsert(bp, "bbb", "v1")
		bt.mustInsert(bp, "ddd", "v2")

		// WHEN: 全レコードより大きい範囲
		count, err := bt.RecordsInRange(bp, []byte("xxx"), []byte("zzz"), true, true)

		// THEN
		require.NoError(t, err)
		assert.Equal(t, int64(0), count)
	})

	t.Run("複数リーフページにまたがる範囲を正しくカウントできる", func(t *testing.T) {
		// GIVEN: 多数のレコードを挿入してノード分割を発生させる
		bt, bp := setupBTree(t)
		numRecords := 100
		for i := range numRecords {
			key := fmt.Sprintf("key_%03d", i)
			bt.mustInsert(bp, key, strings.Repeat("x", 200))
		}

		// ツリーの高さが 1 より大きいことを確認 (分割が発生している)
		height, err := bt.Height(bp)
		require.NoError(t, err)
		require.Greater(t, height, uint64(1), "ノード分割が発生している必要がある")

		// WHEN: 全範囲
		countAll, err := bt.RecordsInRange(bp, nil, nil, true, true)
		require.NoError(t, err)

		// THEN: 全レコード数と一致 (正確なカウントまたは妥当な推定)
		assert.Equal(t, int64(numRecords), countAll)

		// WHEN: 部分範囲 (key_020 <= key <= key_050)
		countPartial, err := bt.RecordsInRange(bp, []byte("key_020"), []byte("key_050"), true, true)
		require.NoError(t, err)

		// THEN: 31 レコード (key_020 から key_050 まで)
		assert.Equal(t, int64(31), countPartial)
	})

	t.Run("隣接する 2 ページにまたがる範囲を正しくカウントできる", func(t *testing.T) {
		// GIVEN: ノード分割が発生する程度のレコード数
		bt, bp := setupBTree(t)
		numRecords := 20
		// value を大きくして少ないレコード数で分割を発生させる
		for i := range numRecords {
			key := fmt.Sprintf("key_%03d", i)
			bt.mustInsert(bp, key, strings.Repeat("x", 500))
		}

		leafCount, err := bt.LeafPageCount(bp)
		require.NoError(t, err)
		require.GreaterOrEqual(t, leafCount, uint64(2), "少なくとも 2 ページに分割される必要がある")

		// WHEN: 全範囲 (隣接ページをまたぐ)
		count, err := bt.RecordsInRange(bp, nil, nil, true, true)
		require.NoError(t, err)

		// THEN
		assert.Equal(t, int64(numRecords), count)
	})

	t.Run("leftIncl と rightIncl の非対称ケースが正しく動作する", func(t *testing.T) {
		// GIVEN
		bt, bp := setupBTree(t)
		bt.mustInsert(bp, "aaa", "v1")
		bt.mustInsert(bp, "bbb", "v2")
		bt.mustInsert(bp, "ccc", "v3")
		bt.mustInsert(bp, "ddd", "v4")

		// WHEN: "aaa" <= key < "ddd" (左含む、右含まない)
		count, err := bt.RecordsInRange(bp, []byte("aaa"), []byte("ddd"), true, false)
		require.NoError(t, err)

		// THEN: aaa, bbb, ccc
		assert.Equal(t, int64(3), count)

		// WHEN: "aaa" < key <= "ddd" (左含まない、右含む)
		count, err = bt.RecordsInRange(bp, []byte("aaa"), []byte("ddd"), false, true)
		require.NoError(t, err)

		// THEN: bbb, ccc, ddd
		assert.Equal(t, int64(3), count)
	})

	t.Run("大量レコードでサンプリングベース推定が妥当な値を返す", func(t *testing.T) {
		// GIVEN: 10 ページ以上に分散するレコード数
		bt, bp := setupBTree(t)
		numRecords := 500
		for i := range numRecords {
			key := fmt.Sprintf("key_%05d", i)
			bt.mustInsert(bp, key, strings.Repeat("x", 200))
		}

		leafCount, err := bt.LeafPageCount(bp)
		require.NoError(t, err)
		require.Greater(t, leafCount, uint64(10), "10 ページ以上に分散する必要がある")

		// WHEN: 広い範囲 (サンプリングベース推定が発動するケース)
		count, err := bt.RecordsInRange(bp, []byte("key_00050"), []byte("key_00450"), true, true)
		require.NoError(t, err)

		// THEN: 推定値が実際の値 (401) の ±50% 以内であること
		expectedApprox := int64(401)
		assert.InDelta(t, expectedApprox, count, float64(expectedApprox)*0.5,
			"サンプリング推定値 %d は期待値 %d の ±50%% 以内であるべき", count, expectedApprox)

		// 上限キャップの確認: 全範囲の推定値がテーブル総行数を超えないこと
		countAll, err := bt.RecordsInRange(bp, nil, nil, true, true)
		require.NoError(t, err)
		assert.LessOrEqual(t, countAll, int64(numRecords),
			"推定値 %d がテーブル総行数 %d を超えてはならない", countAll, numRecords)
	})

	t.Run("下限キーがリーフ末尾を超えた場合に次のリーフに進む", func(t *testing.T) {
		// GIVEN: 複数ページに分割されたツリー
		bt, bp := setupBTree(t)
		numRecords := 100
		for i := range numRecords {
			key := fmt.Sprintf("key_%03d", i)
			bt.mustInsert(bp, key, strings.Repeat("x", 200))
		}

		height, err := bt.Height(bp)
		require.NoError(t, err)
		require.Greater(t, height, uint64(1))

		// WHEN: 下限キーがリーフ内の最大キーとブランチの区切りキーの間に落ちるケース
		// 存在しないキー "key_099z" は "key_099" より大きく、リーフの末尾を超える可能性がある
		count, err := bt.RecordsInRange(bp, []byte("key_099z"), nil, true, true)
		require.NoError(t, err)

		// THEN: 0 ではなく、残りのレコードがカウントされるべき (最低でも 0 以上)
		// key_099z より大きいレコードがなければ 0 は正しいが、
		// "key_099" は最後のキーなので 0 で正しい
		assert.Equal(t, int64(0), count)

		// より一般的なケース: リーフの中間あたりで境界をまたぐ
		// "key_019z" は "key_019" と "key_020" の間 → 残りは key_020 ~ key_099 = 80 件
		count2, err := bt.RecordsInRange(bp, []byte("key_019z"), nil, true, true)
		require.NoError(t, err)
		assert.Equal(t, int64(80), count2)
	})

	t.Run("等値検索 (lower == upper) で 1 を返す", func(t *testing.T) {
		// GIVEN
		bt, bp := setupBTree(t)
		bt.mustInsert(bp, "aaa", "v1")
		bt.mustInsert(bp, "bbb", "v2")
		bt.mustInsert(bp, "ccc", "v3")

		// WHEN: "bbb" = key (lower == upper, both inclusive)
		count, err := bt.RecordsInRange(bp, []byte("bbb"), []byte("bbb"), true, true)

		// THEN
		require.NoError(t, err)
		assert.Equal(t, int64(1), count)
	})
}

func TestLeafPageIds(t *testing.T) {
	t.Run("高さ 1 のツリーでルートの PageId が返される", func(t *testing.T) {
		// GIVEN
		bt, bp := setupBTree(t)
		bt.mustInsert(bp, "aaa", "v1")

		// WHEN
		pageIds, err := bt.LeafPageIds(bp)

		// THEN
		require.NoError(t, err)
		assert.Len(t, pageIds, 1)
	})

	t.Run("複数ページに分割されたツリーで全リーフの PageId が返される", func(t *testing.T) {
		// GIVEN
		bt, bp := setupBTree(t)
		for i := range 100 {
			key := fmt.Sprintf("key_%03d", i)
			bt.mustInsert(bp, key, strings.Repeat("x", 200))
		}

		leafCount, err := bt.LeafPageCount(bp)
		require.NoError(t, err)

		// WHEN
		pageIds, err := bt.LeafPageIds(bp)

		// THEN
		require.NoError(t, err)
		assert.Equal(t, int(leafCount), len(pageIds))
	})

	t.Run("返された PageId で IsPageCached が正しく判定できる", func(t *testing.T) {
		// GIVEN: レコードを挿入してページがキャッシュに載った状態
		bt, bp := setupBTree(t)
		for i := range 50 {
			key := fmt.Sprintf("key_%03d", i)
			bt.mustInsert(bp, key, strings.Repeat("x", 200))
		}

		// WHEN
		pageIds, err := bt.LeafPageIds(bp)
		require.NoError(t, err)

		// THEN: 挿入直後なのでリーフページはキャッシュに載っているはず
		cachedCount := 0
		for _, pid := range pageIds {
			if bp.IsPageCached(pid) {
				cachedCount++
			}
		}
		assert.Greater(t, cachedCount, 0, "少なくとも一部のリーフがキャッシュに載っている")
	})
}

func TestFindLeafPosition(t *testing.T) {
	t.Run("単一リーフページで正しい位置を返す", func(t *testing.T) {
		// GIVEN
		bt, bp := setupBTree(t)
		bt.mustInsert(bp, "aaa", "v1")
		bt.mustInsert(bp, "bbb", "v2")
		bt.mustInsert(bp, "ccc", "v3")

		// WHEN
		pos, err := bt.findLeafPosition(bp, []byte("bbb"))

		// THEN
		require.NoError(t, err)
		assert.Equal(t, 1, pos.slotNum) // bbb は 2 番目 (0-based で 1)
		assert.True(t, pos.found)
		assert.Equal(t, 3, pos.numRecords)
	})

	t.Run("存在しないキーでは found=false を返す", func(t *testing.T) {
		// GIVEN
		bt, bp := setupBTree(t)
		bt.mustInsert(bp, "aaa", "v1")
		bt.mustInsert(bp, "ccc", "v2")

		// WHEN: "bbb" は存在しない
		pos, err := bt.findLeafPosition(bp, []byte("bbb"))

		// THEN
		require.NoError(t, err)
		assert.False(t, pos.found)
		assert.Equal(t, 1, pos.slotNum) // 挿入位置は 1 (aaa の次)
	})
}
