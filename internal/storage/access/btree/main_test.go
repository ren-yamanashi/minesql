package btree

import (
	"bytes"
	"fmt"
	"strings"
	"testing"

	metapage "minesql/internal/storage/access/btree/meta_page"
	"minesql/internal/storage/access/btree/node"
	"minesql/internal/storage/bufferpool"
	"minesql/internal/storage/page"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// --- ログ出力ヘルパー ---

// B+Tree の全データをスキャンし、key=..., value=... 形式でログに書き出す
func writeScanLog(w *strings.Builder, bp *bufferpool.BufferPool, tree *BTree) {
	iter, err := tree.Search(bp, SearchModeStart{})
	if err != nil {
		panic(err)
	}
	count := 0
	for {
		pair, ok, err := iter.Next(bp)
		if err != nil {
			panic(err)
		}
		if !ok {
			break
		}
		fmt.Fprintf(w, "  key=%s, value=%s x %d\n", string(pair.Key), string(pair.Value[:1]), len(pair.Value))
		count++
	}
	fmt.Fprintf(w, "  合計: %d 件\n", count)
}

// ツリーのルートノード情報をログに書き出す (ノードタイプ, キー数, キー一覧)
func writeRootInfo(w *strings.Builder, bp *bufferpool.BufferPool, tree *BTree) {
	metaBuf, err := bp.FetchPage(tree.MetaPageId)
	if err != nil {
		panic(err)
	}
	defer bp.UnRefPage(tree.MetaPageId)

	meta := metapage.NewMetaPage(metaBuf.GetReadData())
	rootPageId := meta.RootPageId()
	writeNodeInfo(w, bp, rootPageId, 0)
}

// ノード情報を再帰的にログに書き出す
func writeNodeInfo(w *strings.Builder, bp *bufferpool.BufferPool, pageId page.PageId, depth int) {
	buf, err := bp.FetchPage(pageId)
	if err != nil {
		panic(err)
	}
	defer bp.UnRefPage(pageId)

	indent := strings.Repeat("  ", depth)
	nodeType := node.GetNodeType(buf.GetReadData())

	if bytes.Equal(nodeType, node.NODE_TYPE_LEAF) {
		leafNode := node.NewLeafNode(buf.GetReadData())
		keys := make([]string, leafNode.NumPairs())
		for i := range leafNode.NumPairs() {
			keys[i] = string(leafNode.PairAt(i).Key)
		}
		fmt.Fprintf(w, "%sLeaf[keys=%d]: [%s]\n", indent, leafNode.NumPairs(), strings.Join(keys, ", "))
	} else if bytes.Equal(nodeType, node.NODE_TYPE_BRANCH) {
		branchNode := node.NewBranchNode(buf.GetReadData())
		keys := make([]string, branchNode.NumPairs())
		for i := range branchNode.NumPairs() {
			keys[i] = string(branchNode.PairAt(i).Key)
		}
		fmt.Fprintf(w, "%sBranch[keys=%d]: [%s]\n", indent, branchNode.NumPairs(), strings.Join(keys, ", "))

		for i := range branchNode.NumPairs() + 1 {
			childPageId := branchNode.ChildPageIdAt(i)
			writeNodeInfo(w, bp, childPageId, depth+1)
		}
	}
}

// ツリーの形状 (高さ、各深さのノードタイプ・ノード数・キー数) をコンパクトに出力する
func writeTreeShape(w *strings.Builder, bp *bufferpool.BufferPool, tree *BTree) {
	metaBuf, err := bp.FetchPage(tree.MetaPageId)
	if err != nil {
		panic(err)
	}
	meta := metapage.NewMetaPage(metaBuf.GetReadData())
	rootPageId := meta.RootPageId()
	bp.UnRefPage(tree.MetaPageId)

	type depthInfo struct {
		nodeType  string
		count     int
		totalKeys int
	}
	result := make(map[int]*depthInfo)

	var collect func(pageId page.PageId, depth int)
	collect = func(pageId page.PageId, depth int) {
		buf, err := bp.FetchPage(pageId)
		if err != nil {
			panic(err)
		}
		defer bp.UnRefPage(pageId)

		nodeType := node.GetNodeType(buf.GetReadData())
		if bytes.Equal(nodeType, node.NODE_TYPE_LEAF) {
			if _, ok := result[depth]; !ok {
				result[depth] = &depthInfo{nodeType: "Leaf"}
			}
			leafNode := node.NewLeafNode(buf.GetReadData())
			result[depth].count++
			result[depth].totalKeys += leafNode.NumPairs()
		} else if bytes.Equal(nodeType, node.NODE_TYPE_BRANCH) {
			if _, ok := result[depth]; !ok {
				result[depth] = &depthInfo{nodeType: "Branch"}
			}
			branchNode := node.NewBranchNode(buf.GetReadData())
			result[depth].count++
			result[depth].totalKeys += branchNode.NumPairs()
			for i := range branchNode.NumPairs() + 1 {
				collect(branchNode.ChildPageIdAt(i), depth+1)
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

// --- テスト ---

func TestBTreeInsertAndScan(t *testing.T) {
	t.Run("20 件のデータを挿入し、全件スキャンで昇順に取得できる", func(t *testing.T) {
		// GIVEN
		tree, bp := setupBTree(t)

		fruits := []string{
			"apple", "banana", "cherry", "date", "elderberry",
			"fig", "grape", "honeydew", "kiwi", "lemon",
			"mango", "nectarine", "orange", "papaya", "quince",
			"raspberry", "strawberry", "tangerine", "ugli", "vanilla",
		}

		// WHEN
		for _, fruit := range fruits {
			tree.mustInsert(bp, fruit, strings.Repeat(string(fruit[0]), 200))
		}

		// THEN
		var w strings.Builder
		writeScanLog(&w, bp, tree)

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
		tree, bp := setupBTree(t)
		for _, fruit := range []string{"apple", "banana", "cherry"} {
			tree.mustInsert(bp, fruit, strings.Repeat(string(fruit[0]), 200))
		}

		// WHEN
		err := bp.FlushPage()
		require.NoError(t, err)

		// THEN
		var w strings.Builder
		writeScanLog(&w, bp, tree)

		expected := `  key=apple, value=a x 200
  key=banana, value=b x 200
  key=cherry, value=c x 200
  合計: 3 件
`
		assert.Equal(t, expected, w.String())
	})
}

func TestBTreeSearchKey(t *testing.T) {
	t.Run("存在するキーを検索できる", func(t *testing.T) {
		// GIVEN
		tree, bp := setupBTree(t)
		for _, fruit := range []string{"apple", "banana", "cherry", "grape", "lemon"} {
			tree.mustInsert(bp, fruit, strings.Repeat(string(fruit[0]), 200))
		}

		// WHEN & THEN
		var w strings.Builder
		for _, key := range []string{"grape", "lemon"} {
			iter, err := tree.Search(bp, SearchModeKey{Key: []byte(key)})
			require.NoError(t, err)
			pair, ok := iter.Get()
			if ok && string(pair.Key) == key {
				fmt.Fprintf(&w, "key=%s, value=%s x %d\n", string(pair.Key), string(pair.Value[:1]), len(pair.Value))
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
		tree, bp := setupBTree(t)
		for _, fruit := range []string{"apple", "banana", "cherry", "grape", "lemon"} {
			tree.mustInsert(bp, fruit, strings.Repeat(string(fruit[0]), 200))
		}

		// WHEN & THEN
		var w strings.Builder
		key := "watermelon"
		iter, err := tree.Search(bp, SearchModeKey{Key: []byte(key)})
		require.NoError(t, err)
		pair, ok := iter.Get()
		if ok && string(pair.Key) == key {
			fmt.Fprintf(&w, "key=%s found\n", key)
		} else {
			fmt.Fprintf(&w, "key=%s not found\n", key)
		}

		assert.Equal(t, "key=watermelon not found\n", w.String())
	})
}

func TestBTreeDeleteIntegration(t *testing.T) {
	t.Run("一部のキーを削除し、残りを確認できる", func(t *testing.T) {
		// GIVEN
		tree, bp := setupBTree(t)
		for _, fruit := range []string{
			"apple", "banana", "cherry", "date", "elderberry",
			"fig", "grape", "honeydew", "kiwi", "lemon",
		} {
			tree.mustInsert(bp, fruit, strings.Repeat(string(fruit[0]), 100))
		}

		// WHEN
		var w strings.Builder
		fmt.Fprintln(&w, "=== 挿入後 ===")
		writeScanLog(&w, bp, tree)

		for _, key := range []string{"banana", "elderberry", "grape"} {
			err := tree.Delete(bp, []byte(key))
			require.NoError(t, err)
			fmt.Fprintf(&w, "Delete: %s\n", key)
		}

		fmt.Fprintln(&w, "=== 削除後 ===")
		writeScanLog(&w, bp, tree)

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
		tree, bp := setupBTree(t)
		tree.mustInsert(bp, "apple", "value")

		// WHEN
		var w strings.Builder
		err := tree.Delete(bp, []byte("banana"))
		fmt.Fprintf(&w, "error: %v\n", err)

		// THEN
		assert.Equal(t, "error: key not found\n", w.String())
	})

	t.Run("削除後に新しいキーを挿入できる", func(t *testing.T) {
		// GIVEN
		tree, bp := setupBTree(t)
		for _, fruit := range []string{"apple", "banana", "cherry"} {
			tree.mustInsert(bp, fruit, strings.Repeat(string(fruit[0]), 100))
		}
		require.NoError(t, tree.Delete(bp, []byte("banana")))

		// WHEN
		err := tree.Insert(bp, node.NewPair([]byte("blueberry"), []byte(strings.Repeat("b", 100))))
		require.NoError(t, err)

		// THEN
		var w strings.Builder
		writeScanLog(&w, bp, tree)

		expected := `  key=apple, value=a x 100
  key=blueberry, value=b x 100
  key=cherry, value=c x 100
  合計: 3 件
`
		assert.Equal(t, expected, w.String())
	})
}

func TestBTreeUpdateIntegration(t *testing.T) {
	t.Run("value のみ更新できる", func(t *testing.T) {
		// GIVEN
		tree, bp := setupBTree(t)
		for _, fruit := range []string{"apple", "banana", "cherry"} {
			tree.mustInsert(bp, fruit, strings.Repeat(string(fruit[0]), 100))
		}

		// WHEN
		var w strings.Builder
		fmt.Fprintln(&w, "=== 更新前 ===")
		writeScanLog(&w, bp, tree)

		require.NoError(t, tree.Update(bp, node.NewPair([]byte("banana"), []byte(strings.Repeat("X", 50)))))

		fmt.Fprintln(&w, "=== 更新後 ===")
		writeScanLog(&w, bp, tree)

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
		tree, bp := setupBTree(t)
		for _, fruit := range []string{"apple", "banana", "cherry"} {
			tree.mustInsert(bp, fruit, strings.Repeat(string(fruit[0]), 100))
		}

		// WHEN
		require.NoError(t, tree.Delete(bp, []byte("apple")))
		require.NoError(t, tree.Insert(bp, node.NewPair([]byte("avocado"), []byte(strings.Repeat("a", 100)))))

		// THEN
		var w strings.Builder
		writeScanLog(&w, bp, tree)

		expected := `  key=avocado, value=a x 100
  key=banana, value=b x 100
  key=cherry, value=c x 100
  合計: 3 件
`
		assert.Equal(t, expected, w.String())
	})

	t.Run("存在しないキーの更新はエラーを返す", func(t *testing.T) {
		// GIVEN
		tree, bp := setupBTree(t)
		tree.mustInsert(bp, "apple", "value")

		// WHEN
		var w strings.Builder
		err := tree.Update(bp, node.NewPair([]byte("banana"), []byte("new_value")))
		fmt.Fprintf(&w, "error: %v\n", err)

		// THEN
		assert.Equal(t, "error: key not found\n", w.String())
	})

	t.Run("同じキーを複数回更新できる", func(t *testing.T) {
		// GIVEN
		tree, bp := setupBTree(t)
		tree.mustInsert(bp, "cherry", strings.Repeat("c", 100))

		// WHEN
		var w strings.Builder
		for i := range 3 {
			newValue := fmt.Sprintf("update_%d_%s", i+1, strings.Repeat("!", 50))
			require.NoError(t, tree.Update(bp, node.NewPair([]byte("cherry"), []byte(newValue))))
			fmt.Fprintf(&w, "Update #%d: len=%d\n", i+1, len(newValue))
		}

		fmt.Fprintln(&w, "=== 最終状態 ===")
		writeScanLog(&w, bp, tree)

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

func TestBTreeNodeSplit(t *testing.T) {
	t.Run("少数の挿入ではルートがリーフノードのまま", func(t *testing.T) {
		// GIVEN
		tree, bp := setupBTree(t)

		// WHEN
		for i := range 5 {
			tree.mustInsert(bp, fmt.Sprintf("key_%02d", i), "v")
		}

		// THEN
		var w strings.Builder
		fmt.Fprintln(&w, "=== ツリー構造 ===")
		writeRootInfo(&w, bp, tree)

		expected := `=== ツリー構造 ===
Leaf[keys=5]: [key_00, key_01, key_02, key_03, key_04]
`
		assert.Equal(t, expected, w.String())
	})

	t.Run("リーフノードが分割されるとルートがブランチノードに昇格する", func(t *testing.T) {
		// GIVEN
		tree, bp := setupBTree(t)

		// WHEN: 大きめの value でリーフノードを溢れさせる (18 件 → 19 件で分割発生)
		for i := range 18 {
			tree.mustInsert(bp, fmt.Sprintf("key_%02d", i), strings.Repeat("x", 200))
		}

		var w strings.Builder

		// 分割前: ルートが 1 つのリーフノード
		fmt.Fprintln(&w, "=== 18 件挿入後 (分割前) ===")
		writeRootInfo(&w, bp, tree)

		// 19 件目の挿入でリーフが溢れ、分割が発生
		tree.mustInsert(bp, "key_18", strings.Repeat("x", 200))
		fmt.Fprintln(&w, "=== 19 件挿入後 (リーフ分割発生) ===")
		writeRootInfo(&w, bp, tree)

		// THEN: 分割前は単一リーフ、分割後はブランチ + 2 リーフに変化
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
		tree, bp := setupBTree(t)

		// 長いキー (100 バイト) + 大きい value でブランチノードも溢れさせる
		keyFn := func(i int) string {
			return fmt.Sprintf("%s_%04d", strings.Repeat("k", 100), i)
		}

		// WHEN: 243 件 → 244 件でブランチ分割が発生
		for i := range 243 {
			tree.mustInsert(bp, keyFn(i), strings.Repeat("x", 200))
		}

		var w strings.Builder

		// 分割前: 高さ 2 (ルート Branch 1 つ + Leaf 34 個)
		fmt.Fprintln(&w, "=== 243 件挿入後 (分割前) ===")
		writeTreeShape(&w, bp, tree)

		// 244 件目の挿入でブランチが溢れ、分割が発生
		tree.mustInsert(bp, keyFn(243), strings.Repeat("x", 200))
		fmt.Fprintln(&w, "=== 244 件挿入後 (ブランチ分割発生) ===")
		writeTreeShape(&w, bp, tree)

		// THEN: 高さ 2 → 3 に変化し、ルートの子にもブランチノードが出現
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
		tree, bp := setupBTree(t)

		// WHEN
		for i := range 20 {
			tree.mustInsert(bp, fmt.Sprintf("key_%02d", i), strings.Repeat("x", 200))
		}

		// THEN: ブランチの境界キーと左右の子のキーの関係をログに出力
		var w strings.Builder
		metaBuf, err := bp.FetchPage(tree.MetaPageId)
		require.NoError(t, err)
		defer bp.UnRefPage(tree.MetaPageId)
		meta := metapage.NewMetaPage(metaBuf.GetReadData())
		rootPageId := meta.RootPageId()

		rootBuf, err := bp.FetchPage(rootPageId)
		require.NoError(t, err)
		defer bp.UnRefPage(rootPageId)

		nodeType := node.GetNodeType(rootBuf.GetReadData())
		if !bytes.Equal(nodeType, node.NODE_TYPE_BRANCH) {
			t.Skip("ルートがブランチではないためスキップ")
		}

		branchNode := node.NewBranchNode(rootBuf.GetReadData())
		for i := range branchNode.NumPairs() {
			boundaryKey := string(branchNode.PairAt(i).Key)

			// 左の子
			leftPageId := branchNode.ChildPageIdAt(i)
			leftBuf, err := bp.FetchPage(leftPageId)
			require.NoError(t, err)
			leftLeaf := node.NewLeafNode(leftBuf.GetReadData())
			lastLeftKey := string(leftLeaf.PairAt(leftLeaf.NumPairs() - 1).Key)
			bp.UnRefPage(leftPageId)

			// 右の子
			rightPageId := branchNode.ChildPageIdAt(i + 1)
			rightBuf, err := bp.FetchPage(rightPageId)
			require.NoError(t, err)
			rightLeaf := node.NewLeafNode(rightBuf.GetReadData())
			firstRightKey := string(rightLeaf.PairAt(0).Key)
			bp.UnRefPage(rightPageId)

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
		tree, bp := setupBTree(t)

		// WHEN: 挿入のたびにルートのノードタイプを記録
		var w strings.Builder
		prevType := ""

		for i := range 30 {
			key := fmt.Sprintf("key_%02d", i)
			tree.mustInsert(bp, key, strings.Repeat("x", 200))

			metaBuf, err := bp.FetchPage(tree.MetaPageId)
			require.NoError(t, err)
			meta := metapage.NewMetaPage(metaBuf.GetReadData())
			rootPageId := meta.RootPageId()
			bp.UnRefPage(tree.MetaPageId)

			rootBuf, err := bp.FetchPage(rootPageId)
			require.NoError(t, err)
			nodeType := node.GetNodeType(rootBuf.GetReadData())
			bp.UnRefPage(rootPageId)

			var currentType string
			if bytes.Equal(nodeType, node.NODE_TYPE_LEAF) {
				currentType = "Leaf"
			} else {
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
