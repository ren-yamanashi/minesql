package btree

import (
	"encoding/binary"
	"minesql/internal/storage/access/btree/node"
	"minesql/internal/storage/disk"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestChildPageId_Start(t *testing.T) {
	t.Run("先頭の子ページIDを正しく取得できる", func(t *testing.T) {
		// GIVEN
		searchMode := SearchModeStart{}
		branchNode := createBranchNode(
			[]node.Pair{
				createPair([]byte("key1"), 100),
				createPair([]byte("key2"), 200),
				createPair([]byte("key3"), 300),
			},
			400,
		)

		// WHEN
		pageId := searchMode.childPageId(branchNode)

		// THEN
		assert.Equal(t, disk.PageId(100), pageId)
	})
}

func TestChildPageId_Key(t *testing.T) {
	t.Run("指定したキーに基づいて子ページIDを正しく取得できる", func(t *testing.T) {
		// GIVEN
		searchMode := SearchModeKey{Key: []byte("key1.5")}
		branchNode := createBranchNode(
			[]node.Pair{
				createPair([]byte("key1"), 100),
				createPair([]byte("key2"), 200),
				createPair([]byte("key3"), 300),
			},
			400,
		)

		// WHEN
		pageId := searchMode.childPageId(branchNode)

		// THEN
		assert.Equal(t, disk.PageId(200), pageId)
	})

	t.Run("検索キーが最小キーより小さい場合、先頭の子ページIDが取得できる", func(t *testing.T) {
		// GIVEN
		branchNode := createBranchNode(
			[]node.Pair{
				createPair([]byte("key1"), 100),
				createPair([]byte("key2"), 200),
				createPair([]byte("key3"), 300),
			},
			400,
		)

		// key0 を検索する場合、key0 < key1 なので、先頭の子ページ (100) が取得される
		searchMode := SearchModeKey{Key: []byte("key0")}

		// WHEN
		pageId := searchMode.childPageId(branchNode)

		// THEN
		assert.Equal(t, disk.PageId(100), pageId)
	})

	t.Run("検索キーが最大キーより大きい場合、右端の子ページIDが取得できる", func(t *testing.T) {
		// GIVEN
		branchNode := createBranchNode(
			[]node.Pair{
				createPair([]byte("key1"), 100),
				createPair([]byte("key2"), 200),
				createPair([]byte("key3"), 300),
			},
			400,
		)

		// key9 を検索する場合、key3 < key9 なので、右端の子ページ (400) が取得される
		searchMode := SearchModeKey{Key: []byte("key9")}

		// WHEN
		pageId := searchMode.childPageId(branchNode)

		// THEN
		assert.Equal(t, disk.PageId(400), pageId)
	})

	t.Run("検索キーが存在する場合、そのキーの右側の子ページIDが取得できる", func(t *testing.T) {
		// GIVEN
		searchMode := SearchModeKey{Key: []byte("key2")}
		branchNode := createBranchNode(
			[]node.Pair{
				createPair([]byte("key1"), 100),
				createPair([]byte("key2"), 200),
				createPair([]byte("key3"), 300),
			},
			400,
		)

		// WHEN
		pageId := searchMode.childPageId(branchNode)

		// THEN
		assert.Equal(t, disk.PageId(300), pageId)
	})
}

func createBranchNode(pairs []node.Pair, rightChildPageId disk.PageId) *node.BranchNode {
	page := &disk.Page{}
	_node := node.NewNode(page[:])
	_node.InitAsBranchNode()
	branchNode := node.NewBranchNode(_node.Body())

	if len(pairs) == 0 {
		panic("pairs must not be empty")
	}

	// 最初のペアを使って初期化
	branchNode.Initialize(pairs[0].Key, disk.PageId(binary.LittleEndian.Uint64(pairs[0].Value)), rightChildPageId)

	// 残りのペアを挿入
	for i := 1; i < len(pairs); i++ {
		if !branchNode.Insert(i, pairs[i]) {
			panic("failed to insert pair")
		}
	}

	return branchNode
}

func createPair(key []byte, pageId disk.PageId) node.Pair {
	value := make([]byte, 8)
	binary.LittleEndian.PutUint64(value, uint64(pageId))
	return node.NewPair(key, value)
}
