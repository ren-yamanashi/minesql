package btree

import (
	"minesql/internal/storage/access/btree/node"
	"minesql/internal/storage/page"
	"testing"

	"github.com/ncw/directio"
	"github.com/stretchr/testify/assert"
)

func TestChildPageId_Start(t *testing.T) {
	t.Run("先頭の子ページIDを正しく取得できる", func(t *testing.T) {
		// GIVEN
		searchMode := SearchModeStart{}
		branchNode := createBranchNode(
			[]node.Pair{
				createPair([]byte("key1"), page.NewPageId(page.FileId(0), page.PageNumber(100))),
				createPair([]byte("key2"), page.NewPageId(page.FileId(0), page.PageNumber(200))),
				createPair([]byte("key3"), page.NewPageId(page.FileId(0), page.PageNumber(300))),
			},
			page.NewPageId(page.FileId(0), page.PageNumber(400)),
		)

		// WHEN
		pageId := searchMode.childPageId(branchNode)

		// THEN
		assert.Equal(t, page.NewPageId(page.FileId(0), page.PageNumber(100)), pageId)
	})
}

func TestChildPageId_Key(t *testing.T) {
	t.Run("指定したキーに基づいて子ページIDを正しく取得できる", func(t *testing.T) {
		// GIVEN
		searchMode := SearchModeKey{Key: []byte("key1.5")}
		branchNode := createBranchNode(
			[]node.Pair{
				createPair([]byte("key1"), page.NewPageId(page.FileId(0), page.PageNumber(100))),
				createPair([]byte("key2"), page.NewPageId(page.FileId(0), page.PageNumber(200))),
				createPair([]byte("key3"), page.NewPageId(page.FileId(0), page.PageNumber(300))),
			},
			page.NewPageId(page.FileId(0), page.PageNumber(400)),
		)

		// WHEN
		pageId := searchMode.childPageId(branchNode)

		// THEN
		assert.Equal(t, page.NewPageId(page.FileId(0), page.PageNumber(200)), pageId)
	})

	t.Run("検索キーが最小キーより小さい場合、先頭の子ページIDが取得できる", func(t *testing.T) {
		// GIVEN
		branchNode := createBranchNode(
			[]node.Pair{
				createPair([]byte("key1"), page.NewPageId(page.FileId(0), page.PageNumber(100))),
				createPair([]byte("key2"), page.NewPageId(page.FileId(0), page.PageNumber(200))),
				createPair([]byte("key3"), page.NewPageId(page.FileId(0), page.PageNumber(300))),
			},
			page.NewPageId(page.FileId(0), page.PageNumber(400)),
		)

		// key0 を検索する場合、key0 < key1 なので、先頭の子ページ (100) が取得される
		searchMode := SearchModeKey{Key: []byte("key0")}

		// WHEN
		pageId := searchMode.childPageId(branchNode)

		// THEN
		assert.Equal(t, page.NewPageId(page.FileId(0), page.PageNumber(100)), pageId)
	})

	t.Run("検索キーが最大キーより大きい場合、右端の子ページIDが取得できる", func(t *testing.T) {
		// GIVEN
		branchNode := createBranchNode(
			[]node.Pair{
				createPair([]byte("key1"), page.NewPageId(page.FileId(0), page.PageNumber(100))),
				createPair([]byte("key2"), page.NewPageId(page.FileId(0), page.PageNumber(200))),
				createPair([]byte("key3"), page.NewPageId(page.FileId(0), page.PageNumber(300))),
			},
			page.NewPageId(page.FileId(0), page.PageNumber(400)),
		)

		// key9 を検索する場合、key3 < key9 なので、右端の子ページ (400) が取得される
		searchMode := SearchModeKey{Key: []byte("key9")}

		// WHEN
		pageId := searchMode.childPageId(branchNode)

		// THEN
		assert.Equal(t, page.NewPageId(page.FileId(0), page.PageNumber(400)), pageId)
	})

	t.Run("検索キーが存在する場合、そのキーの右側の子ページIDが取得できる", func(t *testing.T) {
		// GIVEN
		searchMode := SearchModeKey{Key: []byte("key2")}
		branchNode := createBranchNode(
			[]node.Pair{
				createPair([]byte("key1"), page.NewPageId(page.FileId(0), page.PageNumber(100))),
				createPair([]byte("key2"), page.NewPageId(page.FileId(0), page.PageNumber(200))),
				createPair([]byte("key3"), page.NewPageId(page.FileId(0), page.PageNumber(300))),
			},
			page.NewPageId(page.FileId(0), page.PageNumber(400)),
		)

		// WHEN
		pageId := searchMode.childPageId(branchNode)

		// THEN
		assert.Equal(t, page.NewPageId(page.FileId(0), page.PageNumber(300)), pageId)
	})
}

func createBranchNode(pairs []node.Pair, rightChildPageId page.PageId) *node.BranchNode {
	p := directio.AlignedBlock(directio.BlockSize)
	branchNode := node.NewBranchNode(p)

	if len(pairs) == 0 {
		panic("pairs must not be empty")
	}

	// 最初のペアを使って初期化
	err := branchNode.Initialize(pairs[0].Key, page.PageIdFromBytes(pairs[0].Value), rightChildPageId)
	if err != nil {
		panic("failed to initialize branch node")
	}

	// 残りのペアを挿入
	for i := 1; i < len(pairs); i++ {
		if !branchNode.Insert(i, pairs[i]) {
			panic("failed to insert pair")
		}
	}

	return branchNode
}

func createPair(key []byte, pageId page.PageId) node.Pair {
	return node.NewPair(key, pageId.ToBytes())
}
