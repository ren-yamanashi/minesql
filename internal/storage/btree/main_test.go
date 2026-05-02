package btree

import (
	"bytes"
	"fmt"
	"path/filepath"
	"strings"
	"testing"

	"github.com/ren-yamanashi/minesql/internal/storage/btree/node"
	"github.com/ren-yamanashi/minesql/internal/storage/buffer"
	"github.com/ren-yamanashi/minesql/internal/storage/file"
	"github.com/ren-yamanashi/minesql/internal/storage/page"
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
		writeScanLog(&w, tree)

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
		writeScanLog(&w, tree)

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
		require.NoError(t, tree.Delete([]byte("banana")))

		// WHEN
		err := tree.bufferPool.FlushAllPages()
		require.NoError(t, err)

		// THEN
		var w strings.Builder
		writeScanLog(&w, tree)

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
		for _, key := range []string{"grape", "lemon"} {
			iter, err := tree.Search(SearchModeKey{Key: []byte(key)})
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
		key := "watermelon"
		iter, err := tree.Search(SearchModeKey{Key: []byte(key)})
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
		fmt.Fprintln(&w, "=== 挿入後 ===")
		writeScanLog(&w, tree)

		for _, key := range []string{"banana", "elderberry", "grape"} {
			err := tree.Delete([]byte(key))
			require.NoError(t, err)
			fmt.Fprintf(&w, "Delete: %s\n", key)
		}

		fmt.Fprintln(&w, "=== 削除後 ===")
		writeScanLog(&w, tree)

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
		err := tree.Delete([]byte("banana"))
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
		require.NoError(t, tree.Delete([]byte("banana")))

		// WHEN
		err := tree.Insert(node.NewRecord(nil, []byte("blueberry"), []byte(strings.Repeat("b", 100))))
		require.NoError(t, err)

		// THEN
		var w strings.Builder
		writeScanLog(&w, tree)

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
		fmt.Fprintln(&w, "=== 更新前 ===")
		writeScanLog(&w, tree)

		require.NoError(t, tree.Update(node.NewRecord(nil, []byte("banana"), []byte(strings.Repeat("X", 50)))))

		fmt.Fprintln(&w, "=== 更新後 ===")
		writeScanLog(&w, tree)

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
		require.NoError(t, tree.Delete([]byte("apple")))
		require.NoError(t, tree.Insert(node.NewRecord(nil, []byte("avocado"), []byte(strings.Repeat("a", 100)))))

		// THEN
		var w strings.Builder
		writeScanLog(&w, tree)

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
		err := tree.Update(node.NewRecord(nil, []byte("banana"), []byte("new_value")))
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
		for i := range 3 {
			newValue := fmt.Sprintf("update_%d_%s", i+1, strings.Repeat("!", 50))
			require.NoError(t, tree.Update(node.NewRecord(nil, []byte("cherry"), []byte(newValue))))
			fmt.Fprintf(&w, "Update #%d: len=%d\n", i+1, len(newValue))
		}

		fmt.Fprintln(&w, "=== 最終状態 ===")
		writeScanLog(&w, tree)

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
		pageMeta, err := tree.bufferPool.GetReadPage(tree.MetaPageId)
		require.NoError(t, err)
		meta := newMetaPage(pageMeta)
		rootPageId := meta.rootPageId()

		pageRoot, err := tree.bufferPool.GetReadPage(rootPageId)
		require.NoError(t, err)

		nodeType := node.GetNodeType(pageRoot)
		if !bytes.Equal(nodeType, node.NodeTypeBranch) {
			t.Skip("ルートがブランチではないためスキップ")
		}

		branch := node.NewBranchNode(pageRoot)
		for i := range branch.NumRecords() {
			boundaryKey := string(branch.Record(i).Key())

			// 左の子
			leftPageId, err := branch.ChildPageId(i)
			require.NoError(t, err)
			pageLeaf, err := tree.bufferPool.GetReadPage(leftPageId)
			require.NoError(t, err)
			leftLeaf := node.NewLeafNode(pageLeaf)
			lastLeftKey := string(leftLeaf.Record(leftLeaf.NumRecords() - 1).Key())

			// 右の子
			rightPageId, err := branch.ChildPageId(i + 1)
			require.NoError(t, err)
			pageRight, err := tree.bufferPool.GetReadPage(rightPageId)
			require.NoError(t, err)
			rightLeaf := node.NewLeafNode(pageRight)
			firstRightKey := string(rightLeaf.Record(0).Key())

			fmt.Fprintf(&w, "境界キー: %s\n", boundaryKey)
			fmt.Fprintf(&w, "  左の子の末尾キー: %s (< 境界キー: %v)\n", lastLeftKey, lastLeftKey < boundaryKey)
			fmt.Fprintf(&w, "  右の子の先頭キー: %s (>= 境界キー: %v)\n", firstRightKey, firstRightKey >= boundaryKey)
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

			pageMeta, err := tree.bufferPool.GetReadPage(tree.MetaPageId)
			require.NoError(t, err)
			metaPage := newMetaPage(pageMeta)
			rootPageId := metaPage.rootPageId()

			pageRoot, err := tree.bufferPool.GetReadPage(rootPageId)
			require.NoError(t, err)
			nodeType := node.GetNodeType(pageRoot)

			var currentType string
			switch {
			case bytes.Equal(nodeType, node.NodeTypeLeaf):
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

		var w strings.Builder

		// Insert
		tree.mustInsert("apple", strings.Repeat("a", 100))
		tree.mustInsert("banana", strings.Repeat("b", 100))
		tree.mustInsert("cherry", strings.Repeat("c", 100))
		fmt.Fprintln(&w, "=== Insert 後 ===")
		writeScanLog(&w, tree)

		// Search (FindByKey)
		record, _, err := tree.FindByKey([]byte("banana"))
		require.NoError(t, err)
		fmt.Fprintf(&w, "FindByKey(banana): value=%s x %d\n", string(record.NonKey()[:1]), len(record.NonKey()))

		// Update
		require.NoError(t, tree.Update(node.NewRecord(nil, []byte("banana"), []byte(strings.Repeat("X", 50)))))
		fmt.Fprintln(&w, "=== Update 後 ===")
		writeScanLog(&w, tree)

		// Delete
		require.NoError(t, tree.Delete([]byte("banana")))
		fmt.Fprintln(&w, "=== Delete 後 ===")
		writeScanLog(&w, tree)

		// FindByKey で削除済みキーが見つからない
		_, _, err = tree.FindByKey([]byte("banana"))
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

// B+Tree の全データをスキャンし、key=..., value=... 形式でログに書き出す
func writeScanLog(w *strings.Builder, tree *Btree) {
	iter, err := tree.Search(SearchModeStart{})
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
func writeRootInfo(w *strings.Builder, tree *Btree) {
	pageMeta, err := tree.bufferPool.GetReadPage(tree.MetaPageId)
	if err != nil {
		panic(err)
	}
	writeNodeInfo(w, newMetaPage(pageMeta).rootPageId(), 0, tree)
}

// ノード情報を再帰的にログに書き出す
func writeNodeInfo(w *strings.Builder, pageId page.PageId, depth int, tree *Btree) {
	pg, err := tree.bufferPool.GetReadPage(pageId)
	if err != nil {
		panic(err)
	}

	indent := strings.Repeat("  ", depth)
	nodeType := node.GetNodeType(pg)

	switch {
	case bytes.Equal(nodeType, node.NodeTypeLeaf):
		leafNode := node.NewLeafNode(pg)
		keys := make([]string, leafNode.NumRecords())
		for i := range leafNode.NumRecords() {
			keys[i] = string(leafNode.Record(i).Key())
		}
		fmt.Fprintf(w, "%sLeaf[keys=%d]: [%s]\n", indent, leafNode.NumRecords(), strings.Join(keys, ", "))
	case bytes.Equal(nodeType, node.NodeTypeBranch):
		branchNode := node.NewBranchNode(pg)
		keys := make([]string, branchNode.NumRecords())
		for i := range branchNode.NumRecords() {
			keys[i] = string(branchNode.Record(i).Key())
		}
		fmt.Fprintf(w, "%sBranch[keys=%d]: [%s]\n", indent, branchNode.NumRecords(), strings.Join(keys, ", "))

		for i := range branchNode.NumRecords() + 1 {
			childPageId, err := branchNode.ChildPageId(i)
			if err != nil {
				panic(err)
			}
			writeNodeInfo(w, childPageId, depth+1, tree)
		}
	}
}

// ツリーの形状 (高さ、各深さのノードタイプ・ノード数・キー数) をコンパクトに出力する
func writeTreeShape(w *strings.Builder, tree *Btree) {
	pageMeta, err := tree.bufferPool.GetReadPage(tree.MetaPageId)
	if err != nil {
		panic(err)
	}
	metaPage := newMetaPage(pageMeta)
	rootPageId := metaPage.rootPageId()

	type depthInfo struct {
		nodeType  string
		count     int
		totalKeys int
	}
	result := make(map[int]*depthInfo)

	var collect func(pageId page.PageId, depth int)
	collect = func(pageId page.PageId, depth int) {
		pg, err := tree.bufferPool.GetReadPage(pageId)
		if err != nil {
			panic(err)
		}

		nodeType := node.GetNodeType(pg)
		switch {
		case bytes.Equal(nodeType, node.NodeTypeLeaf):
			if _, ok := result[depth]; !ok {
				result[depth] = &depthInfo{nodeType: "Leaf"}
			}
			leaf := node.NewLeafNode(pg)
			result[depth].count++
			result[depth].totalKeys += leaf.NumRecords()
		case bytes.Equal(nodeType, node.NodeTypeBranch):
			if _, ok := result[depth]; !ok {
				result[depth] = &depthInfo{nodeType: "Branch"}
			}
			branch := node.NewBranchNode(pg)
			result[depth].count++
			result[depth].totalKeys += branch.NumRecords()
			for i := range branch.NumRecords() + 1 {
				childPageId, err := branch.ChildPageId(i)
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
func (bt *Btree) mustInsert(key, value string) {
	record := node.NewRecord([]byte{}, []byte(key), []byte(value))
	err := bt.Insert(record)
	if err != nil {
		panic(fmt.Sprintf("Insert に失敗: %v", err))
	}
}

// setupBtree はテスト用の B+Tree をセットアップする
func setupBtree(t *testing.T) *Btree {
	t.Helper()
	tmpdir := t.TempDir()
	path := filepath.Join(tmpdir, "btree_test.db")
	fileId := page.FileId(0)
	heapFile, err := file.NewHeapFile(fileId, path)
	if err != nil {
		t.Fatalf("HeapFile の作成に失敗: %v", err)
	}
	metaPageId := heapFile.AllocatePageId()

	bp := buffer.NewBufferPool(page.PageSize * 10)
	bp.RegisterHeapFile(fileId, heapFile)

	bt, err := CreateBtree(bp, metaPageId)
	if err != nil {
		t.Fatalf("B+Tree の作成に失敗: %v", err)
	}
	return bt
}
