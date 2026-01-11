package main

import (
	"bytes"
	"fmt"
	"os"
	"strings"

	"minesql/internal/storage/access/btree"
	metapage "minesql/internal/storage/access/btree/meta_page"
	"minesql/internal/storage/access/btree/node"
	"minesql/internal/storage/bufferpool"
	"minesql/internal/storage/disk"
)

func main() {
	dataDir := "examples/btree/data"

	// 既存のデータディレクトリがあれば削除
	os.RemoveAll(dataDir)
	os.MkdirAll(dataDir, 0755)

	bpm := bufferpool.NewBufferPoolManager(10, dataDir)
	fileId := bpm.AllocateFileId()

	// DiskManager を作成して登録
	diskPath := dataDir + "/test.db"
	dm, err := disk.NewDiskManager(fileId, diskPath)
	if err != nil {
		panic(err)
	}
	bpm.RegisterDiskManager(fileId, dm)

	// metaPageId を割り当て
	metaPageId, err := bpm.AllocatePageId(fileId)
	if err != nil {
		panic(err)
	}

	// B+Tree を作成
	tree, err := btree.CreateBTree(bpm, metaPageId)
	if err != nil {
		panic(err)
	}

	// データを挿入
	insertData := []struct {
		key   string
		value string
	}{
		{"apple", strings.Repeat("A", 200)},
		{"banana", strings.Repeat("B", 200)},
		{"cherry", strings.Repeat("C", 200)},
		{"date", strings.Repeat("D", 200)},
		{"elderberry", strings.Repeat("E", 200)},
		{"fig", strings.Repeat("F", 200)},
		{"grape", strings.Repeat("G", 200)},
		{"honeydew", strings.Repeat("H", 200)},
		{"kiwi", strings.Repeat("K", 200)},
		{"lemon", strings.Repeat("L", 200)},
		{"mango", strings.Repeat("M", 200)},
		{"nectarine", strings.Repeat("N", 200)},
		{"orange", strings.Repeat("O", 200)},
		{"papaya", strings.Repeat("P", 200)},
		{"quince", strings.Repeat("Q", 200)},
		{"raspberry", strings.Repeat("R", 200)},
		{"strawberry", strings.Repeat("S", 200)},
		{"tangerine", strings.Repeat("T", 200)},
		{"ugli", strings.Repeat("U", 200)},
		{"vanilla", strings.Repeat("V", 200)},
	}

	for _, data := range insertData {
		fmt.Printf("\nInsert Key: %s, Insert Value: %s\n", data.key, string(data.value[0])+" x "+fmt.Sprint(len(data.value)))
		pair := node.NewPair([]byte(data.key), []byte(data.value))
		err := tree.Insert(bpm, pair)
		if err != nil {
			panic(err)
		}
		displayTreeStructure(bpm, tree)
		fmt.Println()
	}

	// バッファプールの内容をディスクに書き出す
	err = bpm.FlushPage()
	if err != nil {
		panic(err)
	}
}

// B+Tree の構造を表示
func displayTreeStructure(bpm *bufferpool.BufferPoolManager, tree *btree.BTree) {
	fmt.Println("=== B+Tree の構造 ===")

	// メタページからルートページIDを取得
	metaBuf, err := bpm.FetchPage(tree.MetaPageId)
	if err != nil {
		panic(err)
	}
	defer bpm.UnRefPage(tree.MetaPageId)

	meta := metapage.NewMetaPage(metaBuf.Page[:])
	rootPageId := meta.RootPageId()

	// ルートノードを表示
	displayNode(bpm, rootPageId, 0, "Root")
}

// ノードを再帰的に表示
func displayNode(bpm *bufferpool.BufferPoolManager, pageId disk.PageId, depth int, label string) {
	indent := strings.Repeat("  ", depth)

	// ノードを取得
	nodeBuf, err := bpm.FetchPage(pageId)
	if err != nil {
		panic(err)
	}
	defer bpm.UnRefPage(pageId)

	nodeType := node.GetNodeType(nodeBuf.Page[:])

	if bytes.Equal(nodeType, node.NODE_TYPE_LEAF) {
		leafNode := node.NewLeafNode(nodeBuf.Page[:])
		keys := make([]string, 0, leafNode.NumPairs())
		for i := 0; i < leafNode.NumPairs(); i++ {
			pair := leafNode.PairAt(i)
			keys = append(keys, string(pair.Key))
		}
		// リーフノードの情報を表示
		fmt.Printf("%s[%s Leaf: PageID=%v, Keys=[%s]]\n",
			indent, label, pageId, strings.Join(keys, ", "))
	} else if bytes.Equal(nodeType, node.NODE_TYPE_BRANCH) {
		// ブランチノードの場合
		branchNode := node.NewBranchNode(nodeBuf.Page[:])
		keys := make([]string, 0, branchNode.NumPairs())
		for i := 0; i < branchNode.NumPairs(); i++ {
			pair := branchNode.PairAt(i)
			keys = append(keys, string(pair.Key))
		}

		// ブランチノードの情報を表示
		fmt.Printf("%s[%s Branch: PageID=%v, Keys=[%s]]\n",
			indent, label, pageId, strings.Join(keys, ", "))

		// 子ノードを再帰的に表示
		for i := 0; i < branchNode.NumPairs(); i++ {
			childPageId := branchNode.ChildPageIdAt(i)
			pair := branchNode.PairAt(i)
			childLabel := fmt.Sprintf("< %s", string(pair.Key))
			if i > 0 {
				prevPair := branchNode.PairAt(i - 1)
				childLabel = fmt.Sprintf("%s ~ %s", string(prevPair.Key), string(pair.Key))
			}
			displayNode(bpm, childPageId, depth+1, childLabel)
		}

		// 右端の子ノード (ブランチノードのヘッダーから読み取る)
		rightChildPageId := branchNode.ChildPageIdAt(branchNode.NumPairs())
		lastPair := branchNode.PairAt(branchNode.NumPairs() - 1)
		rightLabel := fmt.Sprintf(">= %s", string(lastPair.Key))
		displayNode(bpm, rightChildPageId, depth+1, rightLabel)
	}
}
