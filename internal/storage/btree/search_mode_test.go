package btree

import (
	"minesql/internal/storage/btree/node"
	"minesql/internal/storage/page"
	"testing"

	"github.com/ncw/directio"
	"github.com/stretchr/testify/assert"
)

func TestSearchModeStart(t *testing.T) {
	t.Run("先頭の子ページIDを正しく取得できる", func(t *testing.T) {
		// GIVEN
		searchMode := SearchModeStart{}
		branch := createBranch(
			[]node.Record{
				createRecord([]byte("key1"), page.NewPageId(page.FileId(0), page.PageNumber(100))),
				createRecord([]byte("key2"), page.NewPageId(page.FileId(0), page.PageNumber(200))),
				createRecord([]byte("key3"), page.NewPageId(page.FileId(0), page.PageNumber(300))),
			},
			page.NewPageId(page.FileId(0), page.PageNumber(400)),
		)

		// WHEN
		pageId := searchMode.childPageId(branch)

		// THEN
		assert.Equal(t, page.NewPageId(page.FileId(0), page.PageNumber(100)), pageId)
	})
}

func TestSearchModeKey(t *testing.T) {
	t.Run("指定したキーに基づいて子ページIDを正しく取得できる", func(t *testing.T) {
		// GIVEN
		searchMode := SearchModeKey{Key: []byte("key1.5")}
		branch := createBranch(
			[]node.Record{
				createRecord([]byte("key1"), page.NewPageId(page.FileId(0), page.PageNumber(100))),
				createRecord([]byte("key2"), page.NewPageId(page.FileId(0), page.PageNumber(200))),
				createRecord([]byte("key3"), page.NewPageId(page.FileId(0), page.PageNumber(300))),
			},
			page.NewPageId(page.FileId(0), page.PageNumber(400)),
		)

		// WHEN
		pageId := searchMode.childPageId(branch)

		// THEN
		assert.Equal(t, page.NewPageId(page.FileId(0), page.PageNumber(200)), pageId)
	})

	t.Run("検索キーが最小キーより小さい場合、先頭の子ページIDが取得できる", func(t *testing.T) {
		// GIVEN
		branch := createBranch(
			[]node.Record{
				createRecord([]byte("key1"), page.NewPageId(page.FileId(0), page.PageNumber(100))),
				createRecord([]byte("key2"), page.NewPageId(page.FileId(0), page.PageNumber(200))),
				createRecord([]byte("key3"), page.NewPageId(page.FileId(0), page.PageNumber(300))),
			},
			page.NewPageId(page.FileId(0), page.PageNumber(400)),
		)

		// key0 を検索する場合、key0 < key1 なので、先頭の子ページ (100) が取得される
		searchMode := SearchModeKey{Key: []byte("key0")}

		// WHEN
		pageId := searchMode.childPageId(branch)

		// THEN
		assert.Equal(t, page.NewPageId(page.FileId(0), page.PageNumber(100)), pageId)
	})

	t.Run("検索キーが最大キーより大きい場合、右端の子ページIDが取得できる", func(t *testing.T) {
		// GIVEN
		branch := createBranch(
			[]node.Record{
				createRecord([]byte("key1"), page.NewPageId(page.FileId(0), page.PageNumber(100))),
				createRecord([]byte("key2"), page.NewPageId(page.FileId(0), page.PageNumber(200))),
				createRecord([]byte("key3"), page.NewPageId(page.FileId(0), page.PageNumber(300))),
			},
			page.NewPageId(page.FileId(0), page.PageNumber(400)),
		)

		// key9 を検索する場合、key3 < key9 なので、右端の子ページ (400) が取得される
		searchMode := SearchModeKey{Key: []byte("key9")}

		// WHEN
		pageId := searchMode.childPageId(branch)

		// THEN
		assert.Equal(t, page.NewPageId(page.FileId(0), page.PageNumber(400)), pageId)
	})

	t.Run("検索キーが存在する場合、そのキーの右側の子ページIDが取得できる", func(t *testing.T) {
		// GIVEN
		searchMode := SearchModeKey{Key: []byte("key2")}
		branch := createBranch(
			[]node.Record{
				createRecord([]byte("key1"), page.NewPageId(page.FileId(0), page.PageNumber(100))),
				createRecord([]byte("key2"), page.NewPageId(page.FileId(0), page.PageNumber(200))),
				createRecord([]byte("key3"), page.NewPageId(page.FileId(0), page.PageNumber(300))),
			},
			page.NewPageId(page.FileId(0), page.PageNumber(400)),
		)

		// WHEN
		pageId := searchMode.childPageId(branch)

		// THEN
		assert.Equal(t, page.NewPageId(page.FileId(0), page.PageNumber(300)), pageId)
	})
}

func createBranch(records []node.Record, rightChildPageId page.PageId) *node.Branch {
	p := directio.AlignedBlock(directio.BlockSize)
	branch := node.NewBranch(p)

	if len(records) == 0 {
		panic("records must not be empty")
	}

	// 最初のレコードを使って初期化
	err := branch.Initialize(records[0].KeyBytes(), page.RestorePageIdFromBytes(records[0].NonKeyBytes()), rightChildPageId)
	if err != nil {
		panic("failed to initialize branch node")
	}

	// 残りのレコードを挿入
	for i := 1; i < len(records); i++ {
		if !branch.Insert(i, records[i]) {
			panic("failed to insert record")
		}
	}

	return branch
}

func createRecord(key []byte, pageId page.PageId) node.Record {
	return node.NewRecord(nil, key, pageId.ToBytes())
}
