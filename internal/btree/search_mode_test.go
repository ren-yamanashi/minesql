package btree

import (
	"minesql/internal/btree/node"
	"minesql/internal/storage"
	"testing"

	"github.com/ncw/directio"
	"github.com/stretchr/testify/assert"
)

func TestChildPageId_Start(t *testing.T) {
	t.Run("先頭の子ページIDを正しく取得できる", func(t *testing.T) {
		// GIVEN
		searchMode := SearchModeStart{}
		branchNode := createBranchNode(
			[]node.Record{
				createRecord([]byte("key1"), storage.NewPageId(storage.FileId(0), storage.PageNumber(100))),
				createRecord([]byte("key2"), storage.NewPageId(storage.FileId(0), storage.PageNumber(200))),
				createRecord([]byte("key3"), storage.NewPageId(storage.FileId(0), storage.PageNumber(300))),
			},
			storage.NewPageId(storage.FileId(0), storage.PageNumber(400)),
		)

		// WHEN
		pageId := searchMode.childPageId(branchNode)

		// THEN
		assert.Equal(t, storage.NewPageId(storage.FileId(0), storage.PageNumber(100)), pageId)
	})
}

func TestChildPageId_Key(t *testing.T) {
	t.Run("指定したキーに基づいて子ページIDを正しく取得できる", func(t *testing.T) {
		// GIVEN
		searchMode := SearchModeKey{Key: []byte("key1.5")}
		branchNode := createBranchNode(
			[]node.Record{
				createRecord([]byte("key1"), storage.NewPageId(storage.FileId(0), storage.PageNumber(100))),
				createRecord([]byte("key2"), storage.NewPageId(storage.FileId(0), storage.PageNumber(200))),
				createRecord([]byte("key3"), storage.NewPageId(storage.FileId(0), storage.PageNumber(300))),
			},
			storage.NewPageId(storage.FileId(0), storage.PageNumber(400)),
		)

		// WHEN
		pageId := searchMode.childPageId(branchNode)

		// THEN
		assert.Equal(t, storage.NewPageId(storage.FileId(0), storage.PageNumber(200)), pageId)
	})

	t.Run("検索キーが最小キーより小さい場合、先頭の子ページIDが取得できる", func(t *testing.T) {
		// GIVEN
		branchNode := createBranchNode(
			[]node.Record{
				createRecord([]byte("key1"), storage.NewPageId(storage.FileId(0), storage.PageNumber(100))),
				createRecord([]byte("key2"), storage.NewPageId(storage.FileId(0), storage.PageNumber(200))),
				createRecord([]byte("key3"), storage.NewPageId(storage.FileId(0), storage.PageNumber(300))),
			},
			storage.NewPageId(storage.FileId(0), storage.PageNumber(400)),
		)

		// key0 を検索する場合、key0 < key1 なので、先頭の子ページ (100) が取得される
		searchMode := SearchModeKey{Key: []byte("key0")}

		// WHEN
		pageId := searchMode.childPageId(branchNode)

		// THEN
		assert.Equal(t, storage.NewPageId(storage.FileId(0), storage.PageNumber(100)), pageId)
	})

	t.Run("検索キーが最大キーより大きい場合、右端の子ページIDが取得できる", func(t *testing.T) {
		// GIVEN
		branchNode := createBranchNode(
			[]node.Record{
				createRecord([]byte("key1"), storage.NewPageId(storage.FileId(0), storage.PageNumber(100))),
				createRecord([]byte("key2"), storage.NewPageId(storage.FileId(0), storage.PageNumber(200))),
				createRecord([]byte("key3"), storage.NewPageId(storage.FileId(0), storage.PageNumber(300))),
			},
			storage.NewPageId(storage.FileId(0), storage.PageNumber(400)),
		)

		// key9 を検索する場合、key3 < key9 なので、右端の子ページ (400) が取得される
		searchMode := SearchModeKey{Key: []byte("key9")}

		// WHEN
		pageId := searchMode.childPageId(branchNode)

		// THEN
		assert.Equal(t, storage.NewPageId(storage.FileId(0), storage.PageNumber(400)), pageId)
	})

	t.Run("検索キーが存在する場合、そのキーの右側の子ページIDが取得できる", func(t *testing.T) {
		// GIVEN
		searchMode := SearchModeKey{Key: []byte("key2")}
		branchNode := createBranchNode(
			[]node.Record{
				createRecord([]byte("key1"), storage.NewPageId(storage.FileId(0), storage.PageNumber(100))),
				createRecord([]byte("key2"), storage.NewPageId(storage.FileId(0), storage.PageNumber(200))),
				createRecord([]byte("key3"), storage.NewPageId(storage.FileId(0), storage.PageNumber(300))),
			},
			storage.NewPageId(storage.FileId(0), storage.PageNumber(400)),
		)

		// WHEN
		pageId := searchMode.childPageId(branchNode)

		// THEN
		assert.Equal(t, storage.NewPageId(storage.FileId(0), storage.PageNumber(300)), pageId)
	})
}

func createBranchNode(records []node.Record, rightChildPageId storage.PageId) *node.BranchNode {
	p := directio.AlignedBlock(directio.BlockSize)
	branchNode := node.NewBranchNode(p)

	if len(records) == 0 {
		panic("records must not be empty")
	}

	// 最初のレコードを使って初期化
	err := branchNode.Initialize(records[0].KeyBytes(), storage.RestorePageIdFromBytes(records[0].NonKeyBytes()), rightChildPageId)
	if err != nil {
		panic("failed to initialize branch node")
	}

	// 残りのレコードを挿入
	for i := 1; i < len(records); i++ {
		if !branchNode.Insert(i, records[i]) {
			panic("failed to insert record")
		}
	}

	return branchNode
}

func createRecord(key []byte, pageId storage.PageId) node.Record {
	return node.NewRecord(nil, key, pageId.ToBytes())
}
