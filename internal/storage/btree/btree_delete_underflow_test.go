package btree

import (
	"testing"

	"github.com/ren-yamanashi/minesql/internal/storage/btree/node"
	"github.com/ren-yamanashi/minesql/internal/storage/buffer"
	"github.com/ren-yamanashi/minesql/internal/storage/page"
	"github.com/stretchr/testify/assert"
)

func TestDeleteUnderflow(t *testing.T) {
	t.Run("リーフノードのアンダーフロー: 右の兄弟からレコードを転送", func(t *testing.T) {
		// GIVEN
		bt, bp := setupBtreeForTest(t)

		childPageId, childBufPage := allocateTestPage(t, bp)
		childLeaf := initTestLeafNode(t, bp, childPageId)
		childLeaf.Insert(0, largeLeafRecord(0x10))
		childLeaf.Insert(1, largeLeafRecord(0x15))

		siblingPageId, _ := allocateTestPage(t, bp)
		siblingLeaf := initTestLeafNode(t, bp, siblingPageId)
		siblingLeaf.Insert(0, largeLeafRecord(0x20))
		siblingLeaf.Insert(1, largeLeafRecord(0x30))
		siblingLeaf.Insert(2, largeLeafRecord(0x40))
		siblingLeaf.Insert(3, largeLeafRecord(0x50))

		parentPageId, _ := allocateTestPage(t, bp)
		parentBranch := initTestBranchNode(t, bp, parentPageId, []byte{0x20}, childPageId, siblingPageId)

		// WHEN
		underflow, isLeafMerged, err := bt.deleteUnderflow(parentBranch, childBufPage, 0)

		// THEN
		assert.NoError(t, err)
		assert.False(t, underflow)
		assert.False(t, isLeafMerged)
		assert.Equal(t, 3, childLeaf.NumRecords())
		assert.Equal(t, 3, siblingLeaf.NumRecords())
		assert.Equal(t, []byte{0x20}, childLeaf.Record(2).Key())
		assert.Equal(t, []byte{0x30}, siblingLeaf.Record(0).Key())
		childPageIdAfter, err := parentBranch.ChildPageId(0)
		assert.NoError(t, err)
		assert.Equal(t, childPageId, childPageIdAfter)
	})

	t.Run("リーフノードのアンダーフロー: 左の兄弟からレコードを転送", func(t *testing.T) {
		// GIVEN
		bt, bp := setupBtreeForTest(t)

		siblingPageId, _ := allocateTestPage(t, bp)
		siblingLeaf := initTestLeafNode(t, bp, siblingPageId)
		siblingLeaf.Insert(0, largeLeafRecord(0x10))
		siblingLeaf.Insert(1, largeLeafRecord(0x20))
		siblingLeaf.Insert(2, largeLeafRecord(0x30))
		siblingLeaf.Insert(3, largeLeafRecord(0x40))

		childPageId, childBufPage := allocateTestPage(t, bp)
		childLeaf := initTestLeafNode(t, bp, childPageId)
		childLeaf.Insert(0, largeLeafRecord(0x50))
		childLeaf.Insert(1, largeLeafRecord(0x60))

		parentPageId, _ := allocateTestPage(t, bp)
		parentBranch := initTestBranchNode(t, bp, parentPageId, []byte{0x50}, siblingPageId, childPageId)

		// WHEN (childSlotNum = NumRecords = 1 → 左の兄弟が選ばれる)
		underflow, isLeafMerged, err := bt.deleteUnderflow(parentBranch, childBufPage, 1)

		// THEN
		assert.NoError(t, err)
		assert.False(t, underflow)
		assert.False(t, isLeafMerged)
		assert.Equal(t, 3, childLeaf.NumRecords())
		assert.Equal(t, 3, siblingLeaf.NumRecords())
		assert.Equal(t, []byte{0x40}, childLeaf.Record(0).Key())
		assert.Equal(t, []byte{0x30}, siblingLeaf.Record(2).Key())
		siblingPageIdAfter, err := parentBranch.ChildPageId(0)
		assert.NoError(t, err)
		assert.Equal(t, siblingPageId, siblingPageIdAfter)
	})

	t.Run("リーフノードのアンダーフロー: 左の兄弟とマージ", func(t *testing.T) {
		// GIVEN
		bt, bp := setupBtreeForTest(t)

		siblingPageId, siblingBufPage := allocateTestPage(t, bp)
		siblingLeaf := initTestLeafNode(t, bp, siblingPageId)
		siblingLeaf.Insert(0, largeLeafRecord(0x10))
		siblingLeaf.Insert(1, largeLeafRecord(0x20))
		siblingLeaf.Insert(2, largeLeafRecord(0x30))

		childPageId, childBufPage := allocateTestPage(t, bp)
		childLeaf := initTestLeafNode(t, bp, childPageId)
		childLeaf.Insert(0, largeLeafRecord(0x50))

		// child の次のリーフを作成 (マージ後にリンク更新されることを検証)
		nextPageId, _ := allocateTestPage(t, bp)
		nextLeaf := initTestLeafNode(t, bp, nextPageId)
		childLeaf.SetNextPageId(nextPageId)
		nextLeaf.SetPrevPageId(childPageId)

		parentPageId, _ := allocateTestPage(t, bp)
		parentBranch := initTestBranchNode(t, bp, parentPageId, []byte{0x50}, siblingPageId, childPageId)

		// WHEN (childSlotNum = NumRecords = 1 → 左の兄弟が選ばれる)
		underflow, isLeafMerged, err := bt.deleteUnderflow(parentBranch, childBufPage, 1)

		// THEN
		assert.NoError(t, err)
		assert.True(t, underflow)
		assert.True(t, isLeafMerged)
		assert.Equal(t, 4, siblingLeaf.NumRecords())
		assert.Equal(t, 0, parentBranch.NumRecords())
		assert.Equal(t, siblingBufPage.PageId, parentBranch.RightChildPageId())
		assert.Equal(t, nextPageId, siblingLeaf.NextPageId())
		assert.Equal(t, siblingBufPage.PageId, nextLeaf.PrevPageId())
	})

	t.Run("リーフノードのアンダーフロー: 右の兄弟とマージ (兄弟が RightChild)", func(t *testing.T) {
		// GIVEN
		bt, bp := setupBtreeForTest(t)

		childPageId, childBufPage := allocateTestPage(t, bp)
		childLeaf := initTestLeafNode(t, bp, childPageId)
		childLeaf.Insert(0, largeLeafRecord(0x10))

		siblingPageId, _ := allocateTestPage(t, bp)
		siblingLeaf := initTestLeafNode(t, bp, siblingPageId)
		siblingLeaf.Insert(0, largeLeafRecord(0x20))
		siblingLeaf.Insert(1, largeLeafRecord(0x30))
		siblingLeaf.Insert(2, largeLeafRecord(0x40))

		nextPageId, _ := allocateTestPage(t, bp)
		nextLeaf := initTestLeafNode(t, bp, nextPageId)
		siblingLeaf.SetNextPageId(nextPageId)
		nextLeaf.SetPrevPageId(siblingPageId)

		parentPageId, _ := allocateTestPage(t, bp)
		parentBranch := initTestBranchNode(t, bp, parentPageId, []byte{0x20}, childPageId, siblingPageId)

		// WHEN
		underflow, isLeafMerged, err := bt.deleteUnderflow(parentBranch, childBufPage, 0)

		// THEN
		assert.NoError(t, err)
		assert.True(t, underflow)
		assert.True(t, isLeafMerged)
		assert.Equal(t, 4, childLeaf.NumRecords())
		assert.Equal(t, 0, parentBranch.NumRecords())
		assert.Equal(t, childBufPage.PageId, parentBranch.RightChildPageId())
		assert.Equal(t, nextPageId, childLeaf.NextPageId())
		assert.Equal(t, childBufPage.PageId, nextLeaf.PrevPageId())
	})

	t.Run("リーフノードのアンダーフロー: 右の兄弟とマージ (兄弟が RightChild でない)", func(t *testing.T) {
		// GIVEN
		bt, bp := setupBtreeForTest(t)

		childPageId, childBufPage := allocateTestPage(t, bp)
		childLeaf := initTestLeafNode(t, bp, childPageId)
		childLeaf.Insert(0, largeLeafRecord(0x10))

		siblingPageId, _ := allocateTestPage(t, bp)
		siblingLeaf := initTestLeafNode(t, bp, siblingPageId)
		siblingLeaf.Insert(0, largeLeafRecord(0x20))
		siblingLeaf.Insert(1, largeLeafRecord(0x30))
		siblingLeaf.Insert(2, largeLeafRecord(0x40))

		otherPageId, _ := allocateTestPage(t, bp)
		initTestLeafNode(t, bp, otherPageId)

		parentPageId, _ := allocateTestPage(t, bp)
		parentBranch := initTestBranchNode(t, bp, parentPageId, []byte{0x20}, childPageId, otherPageId)
		parentBranch.Insert(1, node.NewRecord([]byte{}, []byte{0x50}, siblingPageId.ToBytes()))

		// WHEN (childSlotNum=0, sibling=slot1, RightChild=otherPageId)
		underflow, isLeafMerged, err := bt.deleteUnderflow(parentBranch, childBufPage, 0)

		// THEN
		assert.NoError(t, err)
		assert.True(t, underflow)
		assert.True(t, isLeafMerged)
		assert.Equal(t, 4, childLeaf.NumRecords())
		assert.Equal(t, 1, parentBranch.NumRecords())
	})

	t.Run("リーフノードのアンダーフロー: 転送不可かつマージ不可の場合はアンダーフローを許容する", func(t *testing.T) {
		// GIVEN
		bt, bp := setupBtreeForTest(t)

		childPageId, childBufPage := allocateTestPage(t, bp)
		childLeaf := initTestLeafNode(t, bp, childPageId)
		childLeaf.Insert(0, largeLeafRecord(0x10))
		childLeaf.Insert(1, largeLeafRecord(0x15))

		siblingPageId, _ := allocateTestPage(t, bp)
		siblingLeaf := initTestLeafNode(t, bp, siblingPageId)
		siblingLeaf.Insert(0, largeLeafRecord(0x20))
		siblingLeaf.Insert(1, largeLeafRecord(0x30))
		siblingLeaf.Insert(2, largeLeafRecord(0x40))

		parentPageId, _ := allocateTestPage(t, bp)
		parentBranch := initTestBranchNode(t, bp, parentPageId, []byte{0x20}, childPageId, siblingPageId)

		// WHEN (sibling は 3 レコードで転送不可、child は 2 レコードで合計 5 レコード分はマージ不可)
		underflow, isLeafMerged, err := bt.deleteUnderflow(parentBranch, childBufPage, 0)

		// THEN
		assert.NoError(t, err)
		assert.False(t, underflow)
		assert.False(t, isLeafMerged)
		assert.Equal(t, 2, childLeaf.NumRecords())
		assert.Equal(t, 3, siblingLeaf.NumRecords())
	})

	t.Run("ブランチノードのアンダーフロー: 右の兄弟からレコードを転送", func(t *testing.T) {
		// GIVEN
		bt, bp := setupBtreeForTest(t)

		childPageId, childBufPage := allocateTestPage(t, bp)
		childBranch := initTestBranchNode(t, bp, childPageId, largeBranchKey(0x10), page.NewPageId(0, 100), page.NewPageId(0, 101))
		insertLargeBranchRecords(childBranch, 3, 0x20)

		siblingPageId, _ := allocateTestPage(t, bp)
		siblingBranch := initTestBranchNode(t, bp, siblingPageId, largeBranchKey(0x60), page.NewPageId(0, 200), page.NewPageId(0, 201))
		insertLargeBranchRecords(siblingBranch, 5, 0x70)

		parentPageId, _ := allocateTestPage(t, bp)
		parentBranch := initTestBranchNode(t, bp, parentPageId, []byte{0x55}, childPageId, siblingPageId)

		childNumBefore := childBranch.NumRecords()
		siblingNumBefore := siblingBranch.NumRecords()

		// WHEN
		underflow, isLeafMerged, err := bt.deleteUnderflow(parentBranch, childBufPage, 0)

		// THEN
		assert.NoError(t, err)
		assert.False(t, underflow)
		assert.False(t, isLeafMerged)
		assert.Equal(t, childNumBefore+1, childBranch.NumRecords())
		assert.Equal(t, siblingNumBefore-1, siblingBranch.NumRecords())
	})

	t.Run("ブランチノードのアンダーフロー: 左の兄弟からレコードを転送", func(t *testing.T) {
		// GIVEN
		bt, bp := setupBtreeForTest(t)

		siblingPageId, _ := allocateTestPage(t, bp)
		siblingBranch := initTestBranchNode(t, bp, siblingPageId, largeBranchKey(0x10), page.NewPageId(0, 200), page.NewPageId(0, 201))
		insertLargeBranchRecords(siblingBranch, 5, 0x20)

		childPageId, childBufPage := allocateTestPage(t, bp)
		childBranch := initTestBranchNode(t, bp, childPageId, largeBranchKey(0x80), page.NewPageId(0, 100), page.NewPageId(0, 101))
		insertLargeBranchRecords(childBranch, 3, 0x90)

		parentPageId, _ := allocateTestPage(t, bp)
		parentBranch := initTestBranchNode(t, bp, parentPageId, []byte{0x70}, siblingPageId, childPageId)

		childNumBefore := childBranch.NumRecords()
		siblingNumBefore := siblingBranch.NumRecords()

		// WHEN (childSlotNum = NumRecords = 1 → 左の兄弟が選ばれる)
		underflow, isLeafMerged, err := bt.deleteUnderflow(parentBranch, childBufPage, 1)

		// THEN
		assert.NoError(t, err)
		assert.False(t, underflow)
		assert.False(t, isLeafMerged)
		assert.Equal(t, childNumBefore+1, childBranch.NumRecords())
		assert.Equal(t, siblingNumBefore-1, siblingBranch.NumRecords())
	})

	t.Run("ブランチノードのアンダーフロー: 左の兄弟とマージ", func(t *testing.T) {
		// GIVEN
		bt, bp := setupBtreeForTest(t)

		siblingPageId, siblingBufPage := allocateTestPage(t, bp)
		siblingBranch := initTestBranchNode(t, bp, siblingPageId, largeBranchKey(0x10), page.NewPageId(0, 200), page.NewPageId(0, 201))
		insertLargeBranchRecords(siblingBranch, 2, 0x20)

		childPageId, childBufPage := allocateTestPage(t, bp)
		initTestBranchNode(t, bp, childPageId, largeBranchKey(0x70), page.NewPageId(0, 100), page.NewPageId(0, 101))

		parentPageId, _ := allocateTestPage(t, bp)
		parentBranch := initTestBranchNode(t, bp, parentPageId, []byte{0x60}, siblingPageId, childPageId)

		// WHEN (childSlotNum = NumRecords = 1 → 左の兄弟が選ばれる)
		underflow, isLeafMerged, err := bt.deleteUnderflow(parentBranch, childBufPage, 1)

		// THEN
		assert.NoError(t, err)
		assert.True(t, underflow)
		assert.False(t, isLeafMerged)
		assert.Equal(t, 0, parentBranch.NumRecords())
		assert.Equal(t, siblingBufPage.PageId, parentBranch.RightChildPageId())
	})

	t.Run("ブランチノードのアンダーフロー: 右の兄弟とマージ (兄弟が RightChild)", func(t *testing.T) {
		// GIVEN
		bt, bp := setupBtreeForTest(t)

		childPageId, childBufPage := allocateTestPage(t, bp)
		initTestBranchNode(t, bp, childPageId, largeBranchKey(0x10), page.NewPageId(0, 100), page.NewPageId(0, 101))

		siblingPageId, _ := allocateTestPage(t, bp)
		initTestBranchNode(t, bp, siblingPageId, largeBranchKey(0x60), page.NewPageId(0, 200), page.NewPageId(0, 201))

		parentPageId, _ := allocateTestPage(t, bp)
		parentBranch := initTestBranchNode(t, bp, parentPageId, []byte{0x50}, childPageId, siblingPageId)

		// WHEN (childSlotNum=0, sibling=RightChild → childSlotNum+1 == NumRecords)
		underflow, isLeafMerged, err := bt.deleteUnderflow(parentBranch, childBufPage, 0)

		// THEN
		assert.NoError(t, err)
		assert.True(t, underflow)
		assert.False(t, isLeafMerged)
		assert.Equal(t, 0, parentBranch.NumRecords())
		assert.Equal(t, childBufPage.PageId, parentBranch.RightChildPageId())
	})

	t.Run("ブランチノードのアンダーフロー: 右の兄弟とマージ (兄弟が RightChild でない)", func(t *testing.T) {
		// GIVEN
		bt, bp := setupBtreeForTest(t)

		childPageId, childBufPage := allocateTestPage(t, bp)
		initTestBranchNode(t, bp, childPageId, largeBranchKey(0x10), page.NewPageId(0, 100), page.NewPageId(0, 101))

		siblingPageId, _ := allocateTestPage(t, bp)
		initTestBranchNode(t, bp, siblingPageId, largeBranchKey(0x40), page.NewPageId(0, 200), page.NewPageId(0, 201))

		otherPageId, _ := allocateTestPage(t, bp)
		initTestBranchNode(t, bp, otherPageId, largeBranchKey(0xA0), page.NewPageId(0, 300), page.NewPageId(0, 301))

		parentPageId, _ := allocateTestPage(t, bp)
		parentBranch := initTestBranchNode(t, bp, parentPageId, []byte{0x30}, childPageId, otherPageId)
		parentBranch.Insert(1, node.NewRecord([]byte{}, []byte{0x70}, siblingPageId.ToBytes()))

		// WHEN (childSlotNum=0, sibling=slot1, RightChild=otherPageId)
		underflow, isLeafMerged, err := bt.deleteUnderflow(parentBranch, childBufPage, 0)

		// THEN
		assert.NoError(t, err)
		assert.True(t, underflow)
		assert.False(t, isLeafMerged)
		assert.Equal(t, 1, parentBranch.NumRecords())
	})
}

// allocateTestPage はテスト用にページを割り当ててバッファプールに追加する
func allocateTestPage(t *testing.T, bp *buffer.BufferPool) (page.PageId, *buffer.BufferPage) {
	t.Helper()
	pageId, err := bp.AllocatePageId(0)
	assert.NoError(t, err)
	bufPage, err := bp.AddPage(pageId)
	assert.NoError(t, err)
	return pageId, bufPage
}

// initTestLeafNode はテスト用の初期化済みリーフノードを作成する
func initTestLeafNode(t *testing.T, bp *buffer.BufferPool, pageId page.PageId) *node.LeafNode {
	t.Helper()
	pg, err := bp.GetWritePage(pageId)
	assert.NoError(t, err)
	leaf := node.NewLeafNode(pg)
	leaf.Initialize()
	return leaf
}

// initTestBranchNode はテスト用の初期化済みブランチノードを作成する
func initTestBranchNode(
	t *testing.T,
	bp *buffer.BufferPool,
	pageId page.PageId,
	key []byte,
	leftChild, rightChild page.PageId,
) *node.BranchNode {
	t.Helper()
	pg, err := bp.GetWritePage(pageId)
	assert.NoError(t, err)
	branch := node.NewBranchNode(pg)
	err = branch.Initialize(key, leftChild, rightChild)
	assert.NoError(t, err)
	return branch
}

// largeLeafRecord は 900 バイトの nonKey を持つリーフレコードを作成する
func largeLeafRecord(key byte) node.Record {
	return node.NewRecord([]byte{}, []byte{key}, make([]byte, 900))
}

// largeBranchKey は 400 バイトのキーを作成する (先頭バイトで識別)
func largeBranchKey(firstByte byte) []byte {
	key := make([]byte, 400)
	key[0] = firstByte
	return key
}

// insertLargeBranchRecords はブランチノードに指定数の大きいレコードを追加する
func insertLargeBranchRecords(bn *node.BranchNode, count int, startKeyByte byte) {
	for i := range count {
		key := largeBranchKey(startKeyByte + byte(i)*0x10)
		record := node.NewRecord([]byte{}, key, page.NewPageId(0, page.PageNumber(300+i)).ToBytes())
		bn.Insert(bn.NumRecords(), record)
	}
}
