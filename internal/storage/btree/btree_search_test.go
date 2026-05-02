package btree

import (
	"testing"

	"github.com/ren-yamanashi/minesql/internal/storage/btree/node"
	"github.com/stretchr/testify/assert"
)

func TestSearch(t *testing.T) {
	t.Run("SearchModeStart で先頭からイテレータを取得できる", func(t *testing.T) {
		// GIVEN
		bp := setupBtreeTestBufferPool(t)
		metaPageId, _ := bp.AllocatePageId(0)
		bt, _ := CreateBtree(bp, metaPageId)
		insertRecordToBtree(t, bt, []byte{0x10}, []byte{0xAA})
		insertRecordToBtree(t, bt, []byte{0x20}, []byte{0xBB})

		// WHEN
		iter, err := bt.Search(SearchModeStart{})

		// THEN
		assert.NoError(t, err)
		record, ok, _ := iter.Get()
		assert.True(t, ok)
		assert.Equal(t, []byte{0x10}, record.Key())
	})

	t.Run("SearchModeKey で指定したキーからイテレータを取得できる", func(t *testing.T) {
		// GIVEN
		bp := setupBtreeTestBufferPool(t)
		metaPageId, _ := bp.AllocatePageId(0)
		bt, _ := CreateBtree(bp, metaPageId)
		insertRecordToBtree(t, bt, []byte{0x10}, []byte{0xAA})
		insertRecordToBtree(t, bt, []byte{0x20}, []byte{0xBB})

		// WHEN
		iter, err := bt.Search(SearchModeKey{Key: []byte{0x20}})

		// THEN
		assert.NoError(t, err)
		record, ok, _ := iter.Get()
		assert.True(t, ok)
		assert.Equal(t, []byte{0x20}, record.Key())
	})

	t.Run("SearchModeKey で全レコードより大きいキーを指定した場合はレコードなしのイテレータを返す", func(t *testing.T) {
		// GIVEN
		bp := setupBtreeTestBufferPool(t)
		metaPageId, _ := bp.AllocatePageId(0)
		bt, _ := CreateBtree(bp, metaPageId)
		insertRecordToBtree(t, bt, []byte{0x10}, []byte{0xAA})

		// WHEN
		iter, err := bt.Search(SearchModeKey{Key: []byte{0xFF}})

		// THEN
		assert.NoError(t, err)
		_, ok, _ := iter.Get()
		assert.False(t, ok)
	})

	t.Run("空の B+Tree で SearchModeStart を実行するとレコードなしのイテレータを返す", func(t *testing.T) {
		// GIVEN
		bp := setupBtreeTestBufferPool(t)
		metaPageId, _ := bp.AllocatePageId(0)
		bt, _ := CreateBtree(bp, metaPageId)

		// WHEN
		iter, err := bt.Search(SearchModeStart{})

		// THEN
		assert.NoError(t, err)
		_, ok, _ := iter.Get()
		assert.False(t, ok)
	})
}

func TestFindByKey(t *testing.T) {
	t.Run("存在するキーのレコードと位置を返す", func(t *testing.T) {
		// GIVEN
		bp := setupBtreeTestBufferPool(t)
		metaPageId, _ := bp.AllocatePageId(0)
		bt, _ := CreateBtree(bp, metaPageId)
		insertRecordToBtree(t, bt, []byte{0x10}, []byte{0xAA})
		insertRecordToBtree(t, bt, []byte{0x20}, []byte{0xBB})

		// WHEN
		record, position, err := bt.FindByKey([]byte{0x20})

		// THEN
		assert.NoError(t, err)
		assert.Equal(t, []byte{0x20}, record.Key())
		assert.Equal(t, []byte{0xBB}, record.NonKey())
		assert.Equal(t, 1, position.SlotNum)
		assert.False(t, position.PageId.IsInvalid())
	})

	t.Run("先頭のキーを検索できる", func(t *testing.T) {
		// GIVEN
		bp := setupBtreeTestBufferPool(t)
		metaPageId, _ := bp.AllocatePageId(0)
		bt, _ := CreateBtree(bp, metaPageId)
		insertRecordToBtree(t, bt, []byte{0x10}, []byte{0xAA})
		insertRecordToBtree(t, bt, []byte{0x20}, []byte{0xBB})

		// WHEN
		record, position, err := bt.FindByKey([]byte{0x10})

		// THEN
		assert.NoError(t, err)
		assert.Equal(t, []byte{0x10}, record.Key())
		assert.Equal(t, 0, position.SlotNum)
	})

	t.Run("存在しないキーの場合は ErrKeyNotFound を返す", func(t *testing.T) {
		// GIVEN
		bp := setupBtreeTestBufferPool(t)
		metaPageId, _ := bp.AllocatePageId(0)
		bt, _ := CreateBtree(bp, metaPageId)
		insertRecordToBtree(t, bt, []byte{0x10}, []byte{0xAA})

		// WHEN
		_, _, err := bt.FindByKey([]byte{0xFF})

		// THEN
		assert.ErrorIs(t, err, ErrKeyNotFound)
	})

	t.Run("空の B+Tree の場合は ErrKeyNotFound を返す", func(t *testing.T) {
		// GIVEN
		bp := setupBtreeTestBufferPool(t)
		metaPageId, _ := bp.AllocatePageId(0)
		bt, _ := CreateBtree(bp, metaPageId)

		// WHEN
		_, _, err := bt.FindByKey([]byte{0x10})

		// THEN
		assert.ErrorIs(t, err, ErrKeyNotFound)
	})
}

func TestLeafPageIds(t *testing.T) {
	t.Run("高さ 1 の場合はルートページの PageId を返す", func(t *testing.T) {
		// GIVEN
		bp := setupBtreeTestBufferPool(t)
		metaPageId, _ := bp.AllocatePageId(0)
		bt, _ := CreateBtree(bp, metaPageId)

		// WHEN
		pageIds, err := bt.LeafPageIds()

		// THEN
		assert.NoError(t, err)
		assert.Equal(t, 1, len(pageIds))
	})

	t.Run("高さ 2 以上の場合はブランチノードを辿って全リーフの PageId を返す", func(t *testing.T) {
		// GIVEN
		bp := setupBtreeBufferPool(t)
		metaPageId, _ := bp.AllocatePageId(0)
		bt, _ := CreateBtree(bp, metaPageId)
		nonKey := make([]byte, 1500)
		bt.Insert(node.NewRecord([]byte{}, []byte{0x01}, nonKey))
		bt.Insert(node.NewRecord([]byte{}, []byte{0x02}, nonKey))
		bt.Insert(node.NewRecord([]byte{}, []byte{0x03}, nonKey))
		height, _ := bt.Height()
		assert.Equal(t, uint64(2), height)
		leafCount, _ := bt.LeafPageCount()

		// WHEN
		pageIds, err := bt.LeafPageIds()

		// THEN
		assert.NoError(t, err)
		assert.Equal(t, int(leafCount), len(pageIds))
		// 各 PageId が有効であることを確認
		for _, id := range pageIds {
			assert.False(t, id.IsInvalid())
		}
	})
}

// insertRecordToBtree はテスト用に B+Tree のルートリーフノードにレコードを直接挿入する
func insertRecordToBtree(t *testing.T, bt *Btree, key, nonKey []byte) {
	t.Helper()
	pageMeta, err := bt.bufferPool.GetReadPage(bt.MetaPageId)
	assert.NoError(t, err)
	mp := newMetaPage(pageMeta)
	rootPageId := mp.rootPageId()

	pg, err := bt.bufferPool.GetWritePage(rootPageId)
	assert.NoError(t, err)
	ln := node.NewLeafNode(pg)
	slotNum, _ := ln.SearchSlotNum(key)
	ln.Insert(slotNum, node.NewRecord([]byte{}, key, nonKey))
}
