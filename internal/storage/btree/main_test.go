package btree

import (
	"fmt"
	"path/filepath"
	"strings"
	"testing"

	"github.com/ren-yamanashi/minesql/internal/storage/buffer"
	"github.com/ren-yamanashi/minesql/internal/storage/file"
	"github.com/ren-yamanashi/minesql/internal/storage/fsp"
	"github.com/ren-yamanashi/minesql/internal/storage/lock"
	"github.com/ren-yamanashi/minesql/internal/storage/page"
	"github.com/ren-yamanashi/minesql/internal/storage/redo"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBtreeInsertAndScan(t *testing.T) {
	t.Run("20 件のデータを挿入し、全件スキャンで昇順に取得できる", func(t *testing.T) {
		// GIVEN
		tree := setupBtree(t)

		fruits := []string{
			"apple", "banana", "cherry", "date", "elderberry",
			"fig", "grape", "honeydew", "kiwi", "lemon",
			"mango", "nectarine", "orange", "papaya", "quince",
			"raspberry", "strawberry", "tangerine", "ugli", "vanilla",
		}

		// WHEN
		for _, fruit := range fruits {
			tree.mustInsert(fruit, strings.Repeat(string(fruit[0]), 200))
		}

		// THEN
		var w strings.Builder
		scanMtr := buffer.NewMtr(tree.bufferPool)
		defer scanMtr.UnpinAll()
		writeScanLog(&w, tree, scanMtr)

		expected := `  key=apple, value=a x 200
  key=banana, value=b x 200
  key=cherry, value=c x 200
  key=date, value=d x 200
  key=elderberry, value=e x 200
  key=fig, value=f x 200
  key=grape, value=g x 200
  key=honeydew, value=h x 200
  key=kiwi, value=k x 200
  key=lemon, value=l x 200
  key=mango, value=m x 200
  key=nectarine, value=n x 200
  key=orange, value=o x 200
  key=papaya, value=p x 200
  key=quince, value=q x 200
  key=raspberry, value=r x 200
  key=strawberry, value=s x 200
  key=tangerine, value=t x 200
  key=ugli, value=u x 200
  key=vanilla, value=v x 200
  合計: 20 件
`
		assert.Equal(t, expected, w.String())
	})

	t.Run("挿入後にディスクに書き出してから再度読み込める", func(t *testing.T) {
		// GIVEN
		tree := setupBtree(t)
		for _, fruit := range []string{"apple", "banana", "cherry"} {
			tree.mustInsert(fruit, strings.Repeat(string(fruit[0]), 200))
		}

		// WHEN
		err := tree.bufferPool.FlushAllPages()
		require.NoError(t, err)

		// THEN
		var w strings.Builder
		scanMtr := buffer.NewMtr(tree.bufferPool)
		defer scanMtr.UnpinAll()
		writeScanLog(&w, tree, scanMtr)

		expected := `  key=apple, value=a x 200
  key=banana, value=b x 200
  key=cherry, value=c x 200
  合計: 3 件
`
		assert.Equal(t, expected, w.String())
	})

	t.Run("削除後にディスクに書き出してから再度読み込める", func(t *testing.T) {
		// GIVEN
		tree := setupBtree(t)
		for _, fruit := range []string{"apple", "banana", "cherry"} {
			tree.mustInsert(fruit, strings.Repeat(string(fruit[0]), 200))
		}
		mtr := buffer.NewMtr(tree.bufferPool)
		defer mtr.UnpinAll()
		require.NoError(t, tree.Delete(mtr, []byte("banana")))

		// WHEN
		err := tree.bufferPool.FlushAllPages()
		require.NoError(t, err)

		// THEN
		var w strings.Builder
		writeScanLog(&w, tree, mtr)

		expected := `  key=apple, value=a x 200
  key=cherry, value=c x 200
  合計: 2 件
`
		assert.Equal(t, expected, w.String())
	})
}

func TestBtreeSearchKey(t *testing.T) {
	t.Run("存在するキーを検索できる", func(t *testing.T) {
		// GIVEN
		tree := setupBtree(t)
		for _, fruit := range []string{"apple", "banana", "cherry", "grape", "lemon"} {
			tree.mustInsert(fruit, strings.Repeat(string(fruit[0]), 200))
		}

		// WHEN / THEN
		var w strings.Builder
		mtr := buffer.NewMtr(tree.bufferPool)
		defer mtr.UnpinAll()
		for _, key := range []string{"grape", "lemon"} {
			iter, err := tree.Search(mtr, SearchModeKey{Key: []byte(key)})
			require.NoError(t, err)
			record, ok, err := iter.Get()
			assert.NoError(t, err)
			if ok && string(record.Key()) == key {
				fmt.Fprintf(&w, "key=%s, value=%s x %d\n", string(record.Key()), string(record.NonKey()[:1]), len(record.NonKey()))
			} else {
				fmt.Fprintf(&w, "key=%s not found\n", key)
			}
		}

		expected := `key=grape, value=g x 200
key=lemon, value=l x 200
`
		assert.Equal(t, expected, w.String())
	})

	t.Run("存在しないキーを検索すると not found になる", func(t *testing.T) {
		// GIVEN
		tree := setupBtree(t)
		for _, fruit := range []string{"apple", "banana", "cherry", "grape", "lemon"} {
			tree.mustInsert(fruit, strings.Repeat(string(fruit[0]), 200))
		}

		// WHEN / THEN
		var w strings.Builder
		mtr := buffer.NewMtr(tree.bufferPool)
		defer mtr.UnpinAll()
		key := "watermelon"
		iter, err := tree.Search(mtr, SearchModeKey{Key: []byte(key)})
		require.NoError(t, err)
		record, ok, err := iter.Get()
		assert.NoError(t, err)
		if ok && string(record.Key()) == key {
			fmt.Fprintf(&w, "key=%s found\n", key)
		} else {
			fmt.Fprintf(&w, "key=%s not found\n", key)
		}

		assert.Equal(t, "key=watermelon not found\n", w.String())
	})
}

func TestBtreeDeleteIntegration(t *testing.T) {
	t.Run("一部のキーを削除し、残りを確認できる", func(t *testing.T) {
		// GIVEN
		tree := setupBtree(t)
		for _, fruit := range []string{
			"apple", "banana", "cherry", "date", "elderberry",
			"fig", "grape", "honeydew", "kiwi", "lemon",
		} {
			tree.mustInsert(fruit, strings.Repeat(string(fruit[0]), 100))
		}

		// WHEN
		var w strings.Builder
		mtr := buffer.NewMtr(tree.bufferPool)
		defer mtr.UnpinAll()
		fmt.Fprintln(&w, "=== 挿入後 ===")
		writeScanLog(&w, tree, mtr)

		for _, key := range []string{"banana", "elderberry", "grape"} {
			err := tree.Delete(mtr, []byte(key))
			require.NoError(t, err)
			fmt.Fprintf(&w, "Delete: %s\n", key)
		}

		fmt.Fprintln(&w, "=== 削除後 ===")
		writeScanLog(&w, tree, mtr)

		// THEN
		expected := `=== 挿入後 ===
  key=apple, value=a x 100
  key=banana, value=b x 100
  key=cherry, value=c x 100
  key=date, value=d x 100
  key=elderberry, value=e x 100
  key=fig, value=f x 100
  key=grape, value=g x 100
  key=honeydew, value=h x 100
  key=kiwi, value=k x 100
  key=lemon, value=l x 100
  合計: 10 件
Delete: banana
Delete: elderberry
Delete: grape
=== 削除後 ===
  key=apple, value=a x 100
  key=cherry, value=c x 100
  key=date, value=d x 100
  key=fig, value=f x 100
  key=honeydew, value=h x 100
  key=kiwi, value=k x 100
  key=lemon, value=l x 100
  合計: 7 件
`
		assert.Equal(t, expected, w.String())
	})

	t.Run("存在しないキーを削除するとエラーを返す", func(t *testing.T) {
		// GIVEN
		tree := setupBtree(t)
		tree.mustInsert("apple", "value")

		// WHEN
		var w strings.Builder
		mtr := buffer.NewMtr(tree.bufferPool)
		defer mtr.UnpinAll()
		err := tree.Delete(mtr, []byte("banana"))
		fmt.Fprintf(&w, "error: %v\n", err)

		// THEN
		assert.Equal(t, "error: key not found\n", w.String())
	})

	t.Run("削除後に新しいキーを挿入できる", func(t *testing.T) {
		// GIVEN
		tree := setupBtree(t)
		for _, fruit := range []string{"apple", "banana", "cherry"} {
			tree.mustInsert(fruit, strings.Repeat(string(fruit[0]), 100))
		}
		mtr := buffer.NewMtr(tree.bufferPool)
		defer mtr.UnpinAll()
		require.NoError(t, tree.Delete(mtr, []byte("banana")))

		// WHEN
		err := tree.Insert(mtr, NewRecord(nil, []byte("blueberry"), []byte(strings.Repeat("b", 100))))
		require.NoError(t, err)

		// THEN
		var w strings.Builder
		writeScanLog(&w, tree, mtr)

		expected := `  key=apple, value=a x 100
  key=blueberry, value=b x 100
  key=cherry, value=c x 100
  合計: 3 件
`
		assert.Equal(t, expected, w.String())
	})
}

func TestBtreeUpdateIntegration(t *testing.T) {
	t.Run("value のみ更新できる", func(t *testing.T) {
		// GIVEN
		tree := setupBtree(t)
		for _, fruit := range []string{"apple", "banana", "cherry"} {
			tree.mustInsert(fruit, strings.Repeat(string(fruit[0]), 100))
		}

		// WHEN
		var w strings.Builder
		mtr := buffer.NewMtr(tree.bufferPool)
		defer mtr.UnpinAll()
		fmt.Fprintln(&w, "=== 更新前 ===")
		writeScanLog(&w, tree, mtr)

		require.NoError(t, tree.Update(mtr, NewRecord(nil, []byte("banana"), []byte(strings.Repeat("X", 50)))))

		fmt.Fprintln(&w, "=== 更新後 ===")
		writeScanLog(&w, tree, mtr)

		// THEN
		expected := `=== 更新前 ===
  key=apple, value=a x 100
  key=banana, value=b x 100
  key=cherry, value=c x 100
  合計: 3 件
=== 更新後 ===
  key=apple, value=a x 100
  key=banana, value=X x 50
  key=cherry, value=c x 100
  合計: 3 件
`
		assert.Equal(t, expected, w.String())
	})

	t.Run("キーの変更は Delete + Insert で実現できる", func(t *testing.T) {
		// GIVEN
		tree := setupBtree(t)
		for _, fruit := range []string{"apple", "banana", "cherry"} {
			tree.mustInsert(fruit, strings.Repeat(string(fruit[0]), 100))
		}

		// WHEN
		mtr := buffer.NewMtr(tree.bufferPool)
		defer mtr.UnpinAll()
		require.NoError(t, tree.Delete(mtr, []byte("apple")))
		require.NoError(t, tree.Insert(mtr, NewRecord(nil, []byte("avocado"), []byte(strings.Repeat("a", 100)))))

		// THEN
		var w strings.Builder
		writeScanLog(&w, tree, mtr)

		expected := `  key=avocado, value=a x 100
  key=banana, value=b x 100
  key=cherry, value=c x 100
  合計: 3 件
`
		assert.Equal(t, expected, w.String())
	})

	t.Run("存在しないキーの更新はエラーを返す", func(t *testing.T) {
		// GIVEN
		tree := setupBtree(t)
		tree.mustInsert("apple", "value")

		// WHEN
		var w strings.Builder
		mtr := buffer.NewMtr(tree.bufferPool)
		defer mtr.UnpinAll()
		err := tree.Update(mtr, NewRecord(nil, []byte("banana"), []byte("new_value")))
		fmt.Fprintf(&w, "error: %v\n", err)

		// THEN
		assert.Equal(t, "error: key not found\n", w.String())
	})

	t.Run("同じキーを複数回更新できる", func(t *testing.T) {
		// GIVEN
		tree := setupBtree(t)
		tree.mustInsert("cherry", strings.Repeat("c", 100))

		// WHEN
		var w strings.Builder
		mtr := buffer.NewMtr(tree.bufferPool)
		defer mtr.UnpinAll()
		for i := range 3 {
			newValue := fmt.Sprintf("update_%d_%s", i+1, strings.Repeat("!", 50))
			require.NoError(t, tree.Update(mtr, NewRecord(nil, []byte("cherry"), []byte(newValue))))
			fmt.Fprintf(&w, "Update #%d: len=%d\n", i+1, len(newValue))
		}

		fmt.Fprintln(&w, "=== 最終状態 ===")
		writeScanLog(&w, tree, mtr)

		// THEN
		expected := `Update #1: len=59
Update #2: len=59
Update #3: len=59
=== 最終状態 ===
  key=cherry, value=u x 59
  合計: 1 件
`
		assert.Equal(t, expected, w.String())
	})
}

func TestBtreeNodeSplit(t *testing.T) {
	t.Run("少数の挿入ではルートがリーフノードのまま", func(t *testing.T) {
		// GIVEN
		tree := setupBtree(t)

		// WHEN
		for i := range 5 {
			tree.mustInsert(fmt.Sprintf("key_%02d", i), "v")
		}

		// THEN
		var w strings.Builder
		fmt.Fprintln(&w, "=== ツリー構造 ===")
		writeRootInfo(&w, tree)

		expected := `=== ツリー構造 ===
Leaf[keys=5]: [key_00, key_01, key_02, key_03, key_04]
`
		assert.Equal(t, expected, w.String())
	})

	t.Run("リーフノードが分割されるとルートがブランチノードに昇格する", func(t *testing.T) {
		// GIVEN
		tree := setupBtree(t)

		// WHEN: 大きめの value でリーフノードを溢れさせる (18 件 → 19 件で分割発生)
		for i := range 18 {
			tree.mustInsert(fmt.Sprintf("key_%02d", i), strings.Repeat("x", 200))
		}

		var w strings.Builder

		fmt.Fprintln(&w, "=== 18 件挿入後 (分割前) ===")
		writeRootInfo(&w, tree)

		tree.mustInsert("key_18", strings.Repeat("x", 200))
		fmt.Fprintln(&w, "=== 19 件挿入後 (リーフ分割発生) ===")
		writeRootInfo(&w, tree)

		// THEN
		// 分割前は単一リーフ、分割後はブランチ + 2 リーフ
		expected := `=== 18 件挿入後 (分割前) ===
Leaf[keys=18]: [key_00, key_01, key_02, key_03, key_04, key_05, key_06, key_07, key_08, key_09, key_10, key_11, key_12, key_13, key_14, key_15, key_16, key_17]
=== 19 件挿入後 (リーフ分割発生) ===
Branch[keys=1]: [key_10]
  Leaf[keys=10]: [key_00, key_01, key_02, key_03, key_04, key_05, key_06, key_07, key_08, key_09]
  Leaf[keys=9]: [key_10, key_11, key_12, key_13, key_14, key_15, key_16, key_17, key_18]
`
		assert.Equal(t, expected, w.String())
	})

	t.Run("ブランチノードも分割されツリーの高さが 3 になる", func(t *testing.T) {
		// GIVEN
		tree := setupBtree(t)

		// 長いキー (100 バイト) + 大きい value でブランチノードも溢れさせる
		keyFn := func(i int) string {
			return fmt.Sprintf("%s_%04d", strings.Repeat("k", 100), i)
		}

		// WHEN
		// 243 件 → 244 件でブランチ分割が発生
		for i := range 243 {
			tree.mustInsert(keyFn(i), strings.Repeat("x", 200))
		}

		var w strings.Builder

		// 分割前: 高さ 2 (ルート Branch 1 つ + Leaf 34 個)
		fmt.Fprintln(&w, "=== 243 件挿入後 (分割前) ===")
		writeTreeShape(&w, tree)

		// 244 件目の挿入でブランチが溢れ、分割が発生
		tree.mustInsert(keyFn(243), strings.Repeat("x", 200))
		fmt.Fprintln(&w, "=== 244 件挿入後 (ブランチ分割発生) ===")
		writeTreeShape(&w, tree)

		// THEN
		// 高さ 2 → 3 に変化し、ルートの子にもブランチノードが出現
		expected := `=== 243 件挿入後 (分割前) ===
高さ: 2
  depth=0: Branch x 1 (keys=33)
  depth=1: Leaf x 34 (keys=243)
=== 244 件挿入後 (ブランチ分割発生) ===
高さ: 3
  depth=0: Branch x 1 (keys=1)
  depth=1: Branch x 2 (keys=33)
  depth=2: Leaf x 35 (keys=244)
`
		assert.Equal(t, expected, w.String())
	})

	t.Run("リーフノード分割時にブランチの境界キーが子リーフのキー範囲と整合する", func(t *testing.T) {
		// GIVEN
		tree := setupBtree(t)

		// WHEN
		for i := range 20 {
			tree.mustInsert(fmt.Sprintf("key_%02d", i), strings.Repeat("x", 200))
		}

		// THEN: ブランチの境界キーと左右の子のキーの関係をログに出力
		var w strings.Builder
		bufPageMeta, err := tree.bufferPool.Page(tree.metaPageId)
		require.NoError(t, err)
		defer tree.bufferPool.Unpin(tree.metaPageId)
		meta := newMetaPage(bufPageMeta)
		rootPageId := meta.rootPageId()

		bufPageRoot, err := tree.bufferPool.Page(rootPageId)
		require.NoError(t, err)
		defer tree.bufferPool.Unpin(rootPageId)

		nodeType := nodeType(bufPageRoot.Data())
		if nodeType != nodeTypeBranch {
			t.Skip("ルートがブランチではないためスキップ")
		}

		branch := newBranchNode(bufPageRoot)
		for i := range branch.numRecords() {
			boundaryKey := string(branch.record(i).Key())

			// 左の子
			leftPageId, err := branch.childPageId(i)
			require.NoError(t, err)
			bufPageLeaf, err := tree.bufferPool.Page(leftPageId)
			require.NoError(t, err)
			leftLeaf := newLeafNode(bufPageLeaf)
			lastLeftKey := string(leftLeaf.record(leftLeaf.numRecords() - 1).Key())

			// 右の子
			rightPageId, err := branch.childPageId(i + 1)
			require.NoError(t, err)
			bufPageRight, err := tree.bufferPool.Page(rightPageId)
			require.NoError(t, err)
			rightLeaf := newLeafNode(bufPageRight)
			firstRightKey := string(rightLeaf.record(0).Key())

			fmt.Fprintf(&w, "境界キー: %s\n", boundaryKey)
			fmt.Fprintf(&w, "  左の子の末尾キー: %s (< 境界キー: %v)\n", lastLeftKey, lastLeftKey < boundaryKey)
			fmt.Fprintf(&w, "  右の子の先頭キー: %s (>= 境界キー: %v)\n", firstRightKey, firstRightKey >= boundaryKey)

			tree.bufferPool.Unpin(leftPageId)
			tree.bufferPool.Unpin(rightPageId)
		}

		expected := `境界キー: key_10
  左の子の末尾キー: key_09 (< 境界キー: true)
  右の子の先頭キー: key_10 (>= 境界キー: true)
`
		assert.Equal(t, expected, w.String())
	})

	t.Run("挿入ごとにツリーの状態遷移を追跡できる", func(t *testing.T) {
		// GIVEN
		tree := setupBtree(t)

		// WHEN: 挿入のたびにルートのノードタイプを記録
		var w strings.Builder
		var prevType string

		for i := range 30 {
			key := fmt.Sprintf("key_%02d", i)
			tree.mustInsert(key, strings.Repeat("x", 200))

			pageMeta, err := tree.bufferPool.Page(tree.metaPageId)
			require.NoError(t, err)
			bufPageMeta := newMetaPage(pageMeta)
			rootPageId := bufPageMeta.rootPageId()

			bufPageRoot, err := tree.bufferPool.Page(rootPageId)
			require.NoError(t, err)
			nodeType := nodeType(bufPageRoot.Data())
			tree.bufferPool.Unpin(tree.metaPageId)
			tree.bufferPool.Unpin(rootPageId)

			var currentType string
			switch nodeType {
			case nodeTypeLeaf:
				currentType = "Leaf"
			default:
				currentType = "Branch"
			}

			if currentType != prevType {
				fmt.Fprintf(&w, "%d 件目: ルートが %s に変化\n", i+1, currentType)
				prevType = currentType
			}
		}

		expected := `1 件目: ルートが Leaf に変化
19 件目: ルートが Branch に変化
`
		assert.Equal(t, expected, w.String())
	})
}

func TestBtreeCRUDLifecycle(t *testing.T) {
	t.Run("レコードのライフサイクルを通しで追跡できる", func(t *testing.T) {
		// GIVEN
		tree := setupBtree(t)
		mtr := buffer.NewMtr(tree.bufferPool)
		defer mtr.UnpinAll()

		var w strings.Builder

		// Insert
		tree.mustInsert("apple", strings.Repeat("a", 100))
		tree.mustInsert("banana", strings.Repeat("b", 100))
		tree.mustInsert("cherry", strings.Repeat("c", 100))
		fmt.Fprintln(&w, "=== Insert 後 ===")
		writeScanLog(&w, tree, mtr)

		// Search (FindByKey)
		record, _, err := tree.FindByKey(mtr, []byte("banana"))
		require.NoError(t, err)
		fmt.Fprintf(&w, "FindByKey(banana): value=%s x %d\n", string(record.NonKey()[:1]), len(record.NonKey()))

		// Update
		require.NoError(t, tree.Update(mtr, NewRecord(nil, []byte("banana"), []byte(strings.Repeat("X", 50)))))
		fmt.Fprintln(&w, "=== Update 後 ===")
		writeScanLog(&w, tree, mtr)

		// Delete
		require.NoError(t, tree.Delete(mtr, []byte("banana")))
		fmt.Fprintln(&w, "=== Delete 後 ===")
		writeScanLog(&w, tree, mtr)

		// FindByKey で削除済みキーが見つからない
		_, _, err = tree.FindByKey(mtr, []byte("banana"))
		fmt.Fprintf(&w, "FindByKey(banana): %v\n", err)

		expected := `=== Insert 後 ===
  key=apple, value=a x 100
  key=banana, value=b x 100
  key=cherry, value=c x 100
  合計: 3 件
FindByKey(banana): value=b x 100
=== Update 後 ===
  key=apple, value=a x 100
  key=banana, value=X x 50
  key=cherry, value=c x 100
  合計: 3 件
=== Delete 後 ===
  key=apple, value=a x 100
  key=cherry, value=c x 100
  合計: 2 件
FindByKey(banana): key not found
`
		assert.Equal(t, expected, w.String())
	})
}

func TestBtreeNoPinLeak(t *testing.T) {
	t.Run("Insert 完了後 mtr.UnpinAll で Pin が解放される", func(t *testing.T) {
		// GIVEN
		tree := setupBtree(t)

		// WHEN
		mtr := buffer.NewMtr(tree.bufferPool)
		err := tree.Insert(mtr, NewRecord(nil, []byte("apple"), []byte(strings.Repeat("a", 100))))
		require.NoError(t, err)

		// THEN
		mtr.UnpinAll()
		assert.Equal(t, 0, mtr.PinnedCount())
	})

	t.Run("Update 完了後 mtr.UnpinAll で Pin が解放される", func(t *testing.T) {
		// GIVEN
		tree := setupBtree(t)
		tree.mustInsert("apple", strings.Repeat("a", 100))

		// WHEN
		mtr := buffer.NewMtr(tree.bufferPool)
		err := tree.Update(mtr, NewRecord(nil, []byte("apple"), []byte(strings.Repeat("b", 100))))
		require.NoError(t, err)

		// THEN
		mtr.UnpinAll()
		assert.Equal(t, 0, mtr.PinnedCount())
	})

	t.Run("Delete 完了後 mtr.UnpinAll で Pin が解放される", func(t *testing.T) {
		// GIVEN
		tree := setupBtree(t)
		tree.mustInsert("apple", strings.Repeat("a", 100))
		tree.mustInsert("banana", strings.Repeat("b", 100))

		// WHEN
		mtr := buffer.NewMtr(tree.bufferPool)
		err := tree.Delete(mtr, []byte("apple"))
		require.NoError(t, err)

		// THEN
		mtr.UnpinAll()
		assert.Equal(t, 0, mtr.PinnedCount())
	})

	t.Run("Search 完了時点で Tree ラッチが解放され、リーフ Pin だけが mtr に残る", func(t *testing.T) {
		// GIVEN
		tree := setupBtree(t)
		tree.mustInsert("apple", strings.Repeat("a", 100))

		// WHEN
		mtr := buffer.NewMtr(tree.bufferPool)
		_, err := tree.Search(mtr, SearchModeKey{Key: []byte("apple")})
		require.NoError(t, err)

		// THEN
		assert.Equal(t, 1, mtr.PinnedCount())
		assert.Equal(t, 0, mtr.HeldLatchCount())

		mtr.UnpinAll()
		assert.Equal(t, 0, mtr.PinnedCount())
	})
}

// B+Tree の全データをスキャンし、key=..., value=... 形式でログに書き出す
func writeScanLog(w *strings.Builder, tree *Tree, mtr *buffer.Mtr) {
	iter, err := tree.Search(mtr, SearchModeStart{})
	if err != nil {
		panic(err)
	}
	count := 0
	for {
		record, ok, err := iter.Next()
		if err != nil {
			panic(err)
		}
		if !ok {
			break
		}
		fmt.Fprintf(w, "  key=%s, value=%s x %d\n", string(record.Key()), string(record.NonKey()[:1]), len(record.NonKey()))
		count++
	}
	fmt.Fprintf(w, "  合計: %d 件\n", count)
}

// ツリーのルートノード情報をログに書き出す (ノードタイプ, キー数, キー一覧)
func writeRootInfo(w *strings.Builder, tree *Tree) {
	bufPageMeta, err := tree.bufferPool.Page(tree.metaPageId)
	if err != nil {
		panic(err)
	}
	defer tree.bufferPool.Unpin(tree.metaPageId)
	writeNodeInfo(w, newMetaPage(bufPageMeta).rootPageId(), 0, tree)
}

// ノード情報を再帰的にログに書き出す
func writeNodeInfo(w *strings.Builder, pageId page.Id, depth int, tree *Tree) {
	pg, err := tree.bufferPool.Page(pageId)
	if err != nil {
		panic(err)
	}
	defer tree.bufferPool.Unpin(pageId)

	indent := strings.Repeat("  ", depth)
	nodeType := nodeType(pg.Data())

	switch nodeType {
	case nodeTypeLeaf:
		leafNode := newLeafNode(pg)
		keys := make([]string, leafNode.numRecords())
		for i := range leafNode.numRecords() {
			keys[i] = string(leafNode.record(i).Key())
		}
		fmt.Fprintf(w, "%sLeaf[keys=%d]: [%s]\n", indent, leafNode.numRecords(), strings.Join(keys, ", "))
	case nodeTypeBranch:
		branchNode := newBranchNode(pg)
		keys := make([]string, branchNode.numRecords())
		for i := range branchNode.numRecords() {
			keys[i] = string(branchNode.record(i).Key())
		}
		fmt.Fprintf(w, "%sBranch[keys=%d]: [%s]\n", indent, branchNode.numRecords(), strings.Join(keys, ", "))

		for i := range branchNode.numRecords() + 1 {
			childPageId, err := branchNode.childPageId(i)
			if err != nil {
				panic(err)
			}
			writeNodeInfo(w, childPageId, depth+1, tree)
		}
	}
}

// ツリーの形状 (高さ、各深さのノードタイプ・ノード数・キー数) をコンパクトに出力する
func writeTreeShape(w *strings.Builder, tree *Tree) {
	pageMeta, err := tree.bufferPool.Page(tree.metaPageId)
	if err != nil {
		panic(err)
	}
	defer tree.bufferPool.Unpin(tree.metaPageId)
	metaPage := newMetaPage(pageMeta)
	rootPageId := metaPage.rootPageId()

	type depthInfo struct {
		nodeType  string
		count     int
		totalKeys int
	}
	result := make(map[int]*depthInfo)

	var collect func(pageId page.Id, depth int)
	collect = func(pageId page.Id, depth int) {
		pg, err := tree.bufferPool.Page(pageId)
		if err != nil {
			panic(err)
		}
		defer tree.bufferPool.Unpin(pageId)

		nodeType := nodeType(pg.Data())
		switch nodeType {
		case nodeTypeLeaf:
			if _, ok := result[depth]; !ok {
				result[depth] = &depthInfo{nodeType: "Leaf"}
			}
			leaf := newLeafNode(pg)
			result[depth].count++
			result[depth].totalKeys += leaf.numRecords()
		case nodeTypeBranch:
			if _, ok := result[depth]; !ok {
				result[depth] = &depthInfo{nodeType: "Branch"}
			}
			branch := newBranchNode(pg)
			result[depth].count++
			result[depth].totalKeys += branch.numRecords()
			for i := range branch.numRecords() + 1 {
				childPageId, err := branch.childPageId(i)
				if err != nil {
					panic(err)
				}
				collect(childPageId, depth+1)
			}
		}
	}
	collect(rootPageId, 0)

	height := len(result)
	fmt.Fprintf(w, "高さ: %d\n", height)
	for d := 0; d < height; d++ {
		info := result[d]
		fmt.Fprintf(w, "  depth=%d: %s x %d (keys=%d)\n", d, info.nodeType, info.count, info.totalKeys)
	}
}

// レコードを挿入するヘルパー (エラー時は panic)
func (bt *Tree) mustInsert(key, value string) {
	record := NewRecord([]byte{}, []byte(key), []byte(value))
	mtr := buffer.NewMtr(bt.bufferPool)
	defer mtr.UnpinAll()
	if err := bt.Insert(mtr, record); err != nil {
		panic(fmt.Sprintf("Insert に失敗: %v", err))
	}
}

// newTestBufferPool はテスト用のバッファプールを生成する
// 全ページが dirty/pin で追い出し不可になった場合の回復は、追い出す操作自身が実行する
// 救済フラッシュに任せられるため、フラッシュ依頼先 (本番ではページクリーナー) は登録しない
func newTestBufferPool(t *testing.T, size int) *buffer.Pool {
	t.Helper()
	return buffer.NewPool(size, newTestRedoBuffer(t), nil)
}

// setupBtree はテスト用の B+Tree をセットアップする
func setupBtree(t *testing.T) *Tree {
	t.Helper()
	tmpdir := t.TempDir()
	path := filepath.Join(tmpdir, "btree_test.db")
	fileId := page.FileId(0)
	heapFile, err := file.NewHeapFile(path)
	if err != nil {
		t.Fatalf("HeapFile の作成に失敗: %v", err)
	}
	bp := newTestBufferPool(t, page.Size*10)
	bp.RegisterHeapFile(fileId, heapFile)

	bt, err := createTreeForTest(t, bp, fileId)
	if err != nil {
		t.Fatalf("B+Tree の作成に失敗: %v", err)
	}
	return bt
}

// newTestRedoBuffer はテスト用の Redo バッファを生成する
func newTestRedoBuffer(t *testing.T) *redo.Buffer {
	t.Helper()
	rl, err := redo.NewBuffer(t.TempDir())
	if err != nil {
		t.Fatalf("redo.Buffer の作成に失敗: %v", err)
	}
	t.Cleanup(func() { _ = rl.Close() })
	return rl
}

// createTreeForTest はテスト用に CreateTree を呼ぶラッパー (Redo バッファとシステム trxId を内部で用意)
//   - CreateTree が要求する FSP ヘッダーを事前に初期化する (テーブル作成経路の InitHeader 相当)
func createTreeForTest(t *testing.T, bp *buffer.Pool, fileId page.FileId) (*Tree, error) {
	t.Helper()
	initMtr := buffer.NewWriteMtr(bp, lock.SystemReservedTrxId, newTestRedoBuffer(t))
	if err := fsp.InitHeader(initMtr, fileId); err != nil {
		initMtr.UnpinAll()
		return nil, err
	}
	if err := initMtr.Commit(); err != nil {
		return nil, err
	}
	mtr := buffer.NewWriteMtr(bp, lock.SystemReservedTrxId, newTestRedoBuffer(t))
	tree, err := CreateTree(bp, fileId, mtr)
	if err != nil {
		mtr.UnpinAll()
		return nil, err
	}
	if err := mtr.Commit(); err != nil {
		return nil, err
	}
	return tree, nil
}
