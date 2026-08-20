package btree

import (
	"testing"

	"github.com/ren-yamanashi/minesql/internal/storage/buffer"
	"github.com/ren-yamanashi/minesql/internal/storage/fsp"
	"github.com/ren-yamanashi/minesql/internal/storage/lock"
	"github.com/ren-yamanashi/minesql/internal/storage/page"
	"github.com/stretchr/testify/assert"
)

func TestDeleteUnderflow(t *testing.T) {
	t.Run("リーフノードのアンダーフロー: 右の兄弟からレコードを転送", func(t *testing.T) {
		// GIVEN
		bt, bp := setupBtreeForTest(t)

		childPageId, childBufPage := allocateTestPageInLeafSegment(t, bp, bt)
		childLeaf := initTestLeafNode(t, bp, childPageId)
		childLeaf.insert(0, largeLeafRecord(0x10))
		childLeaf.insert(1, largeLeafRecord(0x15))

		siblingPageId, _ := allocateTestPageInLeafSegment(t, bp, bt)
		siblingLeaf := initTestLeafNode(t, bp, siblingPageId)
		siblingLeaf.insert(0, largeLeafRecord(0x20))
		siblingLeaf.insert(1, largeLeafRecord(0x30))
		siblingLeaf.insert(2, largeLeafRecord(0x40))
		siblingLeaf.insert(3, largeLeafRecord(0x50))

		parentPageId, _ := allocateTestPage(t, bp)
		parentBranch := initTestBranchNode(t, bp, parentPageId, []byte{0x20}, childPageId, siblingPageId)

		// WHEN
		mtr := buffer.NewMtr(bt.bufferPool)
		defer mtr.UnpinAll()
		underflow, isLeafMerged, err := bt.deleteUnderflow(mtr, parentBranch, childBufPage, 0)

		// THEN
		assert.NoError(t, err)
		assert.False(t, underflow)
		assert.False(t, isLeafMerged)
		assert.Equal(t, 3, childLeaf.numRecords())
		assert.Equal(t, 3, siblingLeaf.numRecords())
		assert.Equal(t, []byte{0x20}, childLeaf.record(2).Key())
		assert.Equal(t, []byte{0x30}, siblingLeaf.record(0).Key())
		childPageIdAfter, err := parentBranch.childPageId(0)
		assert.NoError(t, err)
		assert.Equal(t, childPageId, childPageIdAfter)
	})

	t.Run("リーフノードのアンダーフロー: 左の兄弟からレコードを転送", func(t *testing.T) {
		// GIVEN
		bt, bp := setupBtreeForTest(t)

		siblingPageId, _ := allocateTestPageInLeafSegment(t, bp, bt)
		siblingLeaf := initTestLeafNode(t, bp, siblingPageId)
		siblingLeaf.insert(0, largeLeafRecord(0x10))
		siblingLeaf.insert(1, largeLeafRecord(0x20))
		siblingLeaf.insert(2, largeLeafRecord(0x30))
		siblingLeaf.insert(3, largeLeafRecord(0x40))

		childPageId, childBufPage := allocateTestPageInLeafSegment(t, bp, bt)
		childLeaf := initTestLeafNode(t, bp, childPageId)
		childLeaf.insert(0, largeLeafRecord(0x50))
		childLeaf.insert(1, largeLeafRecord(0x60))

		parentPageId, _ := allocateTestPage(t, bp)
		parentBranch := initTestBranchNode(t, bp, parentPageId, []byte{0x50}, siblingPageId, childPageId)

		// WHEN (childSlotNum = NumRecords = 1 → 左の兄弟が選ばれる)
		mtr := buffer.NewMtr(bt.bufferPool)
		defer mtr.UnpinAll()
		underflow, isLeafMerged, err := bt.deleteUnderflow(mtr, parentBranch, childBufPage, 1)

		// THEN
		assert.NoError(t, err)
		assert.False(t, underflow)
		assert.False(t, isLeafMerged)
		assert.Equal(t, 3, childLeaf.numRecords())
		assert.Equal(t, 3, siblingLeaf.numRecords())
		assert.Equal(t, []byte{0x40}, childLeaf.record(0).Key())
		assert.Equal(t, []byte{0x30}, siblingLeaf.record(2).Key())
		siblingPageIdAfter, err := parentBranch.childPageId(0)
		assert.NoError(t, err)
		assert.Equal(t, siblingPageId, siblingPageIdAfter)
	})

	t.Run("リーフノードのアンダーフロー: 左の兄弟とマージ", func(t *testing.T) {
		// GIVEN
		bt, bp := setupBtreeForTest(t)

		siblingPageId, siblingBufPage := allocateTestPageInLeafSegment(t, bp, bt)
		siblingLeaf := initTestLeafNode(t, bp, siblingPageId)
		siblingLeaf.insert(0, largeLeafRecord(0x10))
		siblingLeaf.insert(1, largeLeafRecord(0x20))
		siblingLeaf.insert(2, largeLeafRecord(0x30))

		childPageId, childBufPage := allocateTestPageInLeafSegment(t, bp, bt)
		childLeaf := initTestLeafNode(t, bp, childPageId)
		childLeaf.insert(0, largeLeafRecord(0x50))

		// child の次のリーフを作成 (マージ後にリンク更新されることを検証)
		nextPageId, _ := allocateTestPage(t, bp)
		nextLeaf := initTestLeafNode(t, bp, nextPageId)
		childLeaf.setNextPageId(nextPageId)
		nextLeaf.setPrevPageId(childPageId)

		parentPageId, _ := allocateTestPage(t, bp)
		parentBranch := initTestBranchNode(t, bp, parentPageId, []byte{0x50}, siblingPageId, childPageId)

		// WHEN (childSlotNum = NumRecords = 1 → 左の兄弟が選ばれる)
		mtr := buffer.NewMtr(bt.bufferPool)
		defer mtr.UnpinAll()
		underflow, isLeafMerged, err := bt.deleteUnderflow(mtr, parentBranch, childBufPage, 1)

		// THEN
		assert.NoError(t, err)
		assert.True(t, underflow)
		assert.True(t, isLeafMerged)
		assert.Equal(t, 4, siblingLeaf.numRecords())
		assert.Equal(t, 0, parentBranch.numRecords())
		assert.Equal(t, siblingBufPage.PageId(), parentBranch.rightChildPageId())
		assert.Equal(t, nextPageId, siblingLeaf.nextPageId())
		assert.Equal(t, siblingBufPage.PageId(), nextLeaf.prevPageId())
		isFree, err := fsp.IsPageFree(mtr, childPageId)
		assert.NoError(t, err)
		assert.True(t, isFree)
	})

	t.Run("リーフノードのアンダーフロー: 右の兄弟とマージ (兄弟が RightChild)", func(t *testing.T) {
		// GIVEN
		bt, bp := setupBtreeForTest(t)

		childPageId, childBufPage := allocateTestPageInLeafSegment(t, bp, bt)
		childLeaf := initTestLeafNode(t, bp, childPageId)
		childLeaf.insert(0, largeLeafRecord(0x10))

		siblingPageId, _ := allocateTestPageInLeafSegment(t, bp, bt)
		siblingLeaf := initTestLeafNode(t, bp, siblingPageId)
		siblingLeaf.insert(0, largeLeafRecord(0x20))
		siblingLeaf.insert(1, largeLeafRecord(0x30))
		siblingLeaf.insert(2, largeLeafRecord(0x40))

		nextPageId, _ := allocateTestPage(t, bp)
		nextLeaf := initTestLeafNode(t, bp, nextPageId)
		siblingLeaf.setNextPageId(nextPageId)
		nextLeaf.setPrevPageId(siblingPageId)

		parentPageId, _ := allocateTestPage(t, bp)
		parentBranch := initTestBranchNode(t, bp, parentPageId, []byte{0x20}, childPageId, siblingPageId)

		// WHEN
		mtr := buffer.NewMtr(bt.bufferPool)
		defer mtr.UnpinAll()
		underflow, isLeafMerged, err := bt.deleteUnderflow(mtr, parentBranch, childBufPage, 0)

		// THEN
		assert.NoError(t, err)
		assert.True(t, underflow)
		assert.True(t, isLeafMerged)
		assert.Equal(t, 4, childLeaf.numRecords())
		assert.Equal(t, 0, parentBranch.numRecords())
		assert.Equal(t, childBufPage.PageId(), parentBranch.rightChildPageId())
		assert.Equal(t, nextPageId, childLeaf.nextPageId())
		assert.Equal(t, childBufPage.PageId(), nextLeaf.prevPageId())
		isFree, err := fsp.IsPageFree(mtr, siblingPageId)
		assert.NoError(t, err)
		assert.True(t, isFree)
	})

	t.Run("リーフノードのアンダーフロー: 右の兄弟とマージ (兄弟が RightChild でない)", func(t *testing.T) {
		// GIVEN
		bt, bp := setupBtreeForTest(t)

		childPageId, childBufPage := allocateTestPageInLeafSegment(t, bp, bt)
		childLeaf := initTestLeafNode(t, bp, childPageId)
		childLeaf.insert(0, largeLeafRecord(0x10))

		siblingPageId, _ := allocateTestPageInLeafSegment(t, bp, bt)
		siblingLeaf := initTestLeafNode(t, bp, siblingPageId)
		siblingLeaf.insert(0, largeLeafRecord(0x20))
		siblingLeaf.insert(1, largeLeafRecord(0x30))
		siblingLeaf.insert(2, largeLeafRecord(0x40))

		otherPageId, _ := allocateTestPage(t, bp)
		initTestLeafNode(t, bp, otherPageId)

		parentPageId, _ := allocateTestPage(t, bp)
		parentBranch := initTestBranchNode(t, bp, parentPageId, []byte{0x20}, childPageId, otherPageId)
		parentBranch.insert(1, NewRecord([]byte{}, []byte{0x50}, siblingPageId.Bytes()))

		// WHEN (childSlotNum=0, sibling=slot1, RightChild=otherPageId)
		mtr := buffer.NewMtr(bt.bufferPool)
		defer mtr.UnpinAll()
		underflow, isLeafMerged, err := bt.deleteUnderflow(mtr, parentBranch, childBufPage, 0)

		// THEN
		assert.NoError(t, err)
		assert.True(t, underflow)
		assert.True(t, isLeafMerged)
		assert.Equal(t, 4, childLeaf.numRecords())
		assert.Equal(t, 1, parentBranch.numRecords())
		isFree, err := fsp.IsPageFree(mtr, siblingPageId)
		assert.NoError(t, err)
		assert.True(t, isFree)
	})

	t.Run("リーフノードのアンダーフロー: 転送不可かつマージ不可の場合はアンダーフローを許容する", func(t *testing.T) {
		// GIVEN
		bt, bp := setupBtreeForTest(t)

		childPageId, childBufPage := allocateTestPage(t, bp)
		childLeaf := initTestLeafNode(t, bp, childPageId)
		childLeaf.insert(0, largeLeafRecord(0x10))
		childLeaf.insert(1, largeLeafRecord(0x15))

		siblingPageId, _ := allocateTestPage(t, bp)
		siblingLeaf := initTestLeafNode(t, bp, siblingPageId)
		siblingLeaf.insert(0, largeLeafRecord(0x20))
		siblingLeaf.insert(1, largeLeafRecord(0x30))
		siblingLeaf.insert(2, largeLeafRecord(0x40))

		parentPageId, _ := allocateTestPage(t, bp)
		parentBranch := initTestBranchNode(t, bp, parentPageId, []byte{0x20}, childPageId, siblingPageId)

		// WHEN (sibling は 3 レコードで転送不可、child は 2 レコードで合計 5 レコード分はマージ不可)
		mtr := buffer.NewMtr(bt.bufferPool)
		defer mtr.UnpinAll()
		underflow, isLeafMerged, err := bt.deleteUnderflow(mtr, parentBranch, childBufPage, 0)

		// THEN
		assert.NoError(t, err)
		assert.False(t, underflow)
		assert.False(t, isLeafMerged)
		assert.Equal(t, 2, childLeaf.numRecords())
		assert.Equal(t, 3, siblingLeaf.numRecords())
	})

	t.Run("ブランチノードのアンダーフロー: 右の兄弟からレコードを転送", func(t *testing.T) {
		// GIVEN
		bt, bp := setupBtreeForTest(t)

		childPageId, childBufPage := allocateTestPage(t, bp)
		childBranch := initTestBranchNode(t, bp, childPageId, largeBranchKey(0x10), page.NewId(0, 100), page.NewId(0, 101))
		insertLargeBranchRecords(childBranch, 3, 0x20)

		siblingPageId, _ := allocateTestPage(t, bp)
		siblingBranch := initTestBranchNode(t, bp, siblingPageId, largeBranchKey(0x60), page.NewId(0, 200), page.NewId(0, 201))
		insertLargeBranchRecords(siblingBranch, 5, 0x70)

		parentPageId, _ := allocateTestPage(t, bp)
		parentBranch := initTestBranchNode(t, bp, parentPageId, []byte{0x55}, childPageId, siblingPageId)

		childNumBefore := childBranch.numRecords()
		siblingNumBefore := siblingBranch.numRecords()

		// WHEN
		mtr := buffer.NewMtr(bt.bufferPool)
		defer mtr.UnpinAll()
		underflow, isLeafMerged, err := bt.deleteUnderflow(mtr, parentBranch, childBufPage, 0)

		// THEN
		assert.NoError(t, err)
		assert.False(t, underflow)
		assert.False(t, isLeafMerged)
		assert.Equal(t, childNumBefore+1, childBranch.numRecords())
		assert.Equal(t, siblingNumBefore-1, siblingBranch.numRecords())
	})

	t.Run("ブランチノードのアンダーフロー: 左の兄弟からレコードを転送", func(t *testing.T) {
		// GIVEN
		bt, bp := setupBtreeForTest(t)

		siblingPageId, _ := allocateTestPage(t, bp)
		siblingBranch := initTestBranchNode(t, bp, siblingPageId, largeBranchKey(0x10), page.NewId(0, 200), page.NewId(0, 201))
		insertLargeBranchRecords(siblingBranch, 5, 0x20)

		childPageId, childBufPage := allocateTestPage(t, bp)
		childBranch := initTestBranchNode(t, bp, childPageId, largeBranchKey(0x80), page.NewId(0, 100), page.NewId(0, 101))
		insertLargeBranchRecords(childBranch, 3, 0x90)

		parentPageId, _ := allocateTestPage(t, bp)
		parentBranch := initTestBranchNode(t, bp, parentPageId, []byte{0x70}, siblingPageId, childPageId)

		childNumBefore := childBranch.numRecords()
		siblingNumBefore := siblingBranch.numRecords()

		// WHEN (childSlotNum = NumRecords = 1 → 左の兄弟が選ばれる)
		mtr := buffer.NewMtr(bt.bufferPool)
		defer mtr.UnpinAll()
		underflow, isLeafMerged, err := bt.deleteUnderflow(mtr, parentBranch, childBufPage, 1)

		// THEN
		assert.NoError(t, err)
		assert.False(t, underflow)
		assert.False(t, isLeafMerged)
		assert.Equal(t, childNumBefore+1, childBranch.numRecords())
		assert.Equal(t, siblingNumBefore-1, siblingBranch.numRecords())
	})

	t.Run("ブランチノードのアンダーフロー: 左の兄弟とマージ", func(t *testing.T) {
		// GIVEN
		bt, bp := setupBtreeForTest(t)

		siblingPageId, siblingBufPage := allocateTestPageInBranchSegment(t, bp, bt)
		siblingBranch := initTestBranchNode(t, bp, siblingPageId, largeBranchKey(0x10), page.NewId(0, 200), page.NewId(0, 201))
		insertLargeBranchRecords(siblingBranch, 2, 0x20)

		childPageId, childBufPage := allocateTestPageInBranchSegment(t, bp, bt)
		initTestBranchNode(t, bp, childPageId, largeBranchKey(0x70), page.NewId(0, 100), page.NewId(0, 101))

		parentPageId, _ := allocateTestPage(t, bp)
		parentBranch := initTestBranchNode(t, bp, parentPageId, []byte{0x60}, siblingPageId, childPageId)

		// WHEN (childSlotNum = NumRecords = 1 → 左の兄弟が選ばれる)
		mtr := buffer.NewMtr(bt.bufferPool)
		defer mtr.UnpinAll()
		underflow, isLeafMerged, err := bt.deleteUnderflow(mtr, parentBranch, childBufPage, 1)

		// THEN
		assert.NoError(t, err)
		assert.True(t, underflow)
		assert.False(t, isLeafMerged)
		assert.Equal(t, 0, parentBranch.numRecords())
		assert.Equal(t, siblingBufPage.PageId(), parentBranch.rightChildPageId())
		isFree, err := fsp.IsPageFree(mtr, childPageId)
		assert.NoError(t, err)
		assert.True(t, isFree)
	})

	t.Run("ブランチノードのアンダーフロー: 右の兄弟とマージ (兄弟が RightChild)", func(t *testing.T) {
		// GIVEN
		bt, bp := setupBtreeForTest(t)

		childPageId, childBufPage := allocateTestPageInBranchSegment(t, bp, bt)
		initTestBranchNode(t, bp, childPageId, largeBranchKey(0x10), page.NewId(0, 100), page.NewId(0, 101))

		siblingPageId, _ := allocateTestPageInBranchSegment(t, bp, bt)
		initTestBranchNode(t, bp, siblingPageId, largeBranchKey(0x60), page.NewId(0, 200), page.NewId(0, 201))

		parentPageId, _ := allocateTestPage(t, bp)
		parentBranch := initTestBranchNode(t, bp, parentPageId, []byte{0x50}, childPageId, siblingPageId)

		// WHEN (childSlotNum=0, sibling=RightChild → childSlotNum+1 == NumRecords)
		mtr := buffer.NewMtr(bt.bufferPool)
		defer mtr.UnpinAll()
		underflow, isLeafMerged, err := bt.deleteUnderflow(mtr, parentBranch, childBufPage, 0)

		// THEN
		assert.NoError(t, err)
		assert.True(t, underflow)
		assert.False(t, isLeafMerged)
		assert.Equal(t, 0, parentBranch.numRecords())
		assert.Equal(t, childBufPage.PageId(), parentBranch.rightChildPageId())
		isFree, err := fsp.IsPageFree(mtr, siblingPageId)
		assert.NoError(t, err)
		assert.True(t, isFree)
	})

	t.Run("ブランチノードのアンダーフロー: 右の兄弟とマージ (兄弟が RightChild でない)", func(t *testing.T) {
		// GIVEN
		bt, bp := setupBtreeForTest(t)

		childPageId, childBufPage := allocateTestPageInBranchSegment(t, bp, bt)
		initTestBranchNode(t, bp, childPageId, largeBranchKey(0x10), page.NewId(0, 100), page.NewId(0, 101))

		siblingPageId, _ := allocateTestPageInBranchSegment(t, bp, bt)
		initTestBranchNode(t, bp, siblingPageId, largeBranchKey(0x40), page.NewId(0, 200), page.NewId(0, 201))

		otherPageId, _ := allocateTestPage(t, bp)
		initTestBranchNode(t, bp, otherPageId, largeBranchKey(0xA0), page.NewId(0, 300), page.NewId(0, 301))

		parentPageId, _ := allocateTestPage(t, bp)
		parentBranch := initTestBranchNode(t, bp, parentPageId, []byte{0x30}, childPageId, otherPageId)
		parentBranch.insert(1, NewRecord([]byte{}, []byte{0x70}, siblingPageId.Bytes()))

		// WHEN (childSlotNum=0, sibling=slot1, RightChild=otherPageId)
		mtr := buffer.NewMtr(bt.bufferPool)
		defer mtr.UnpinAll()
		underflow, isLeafMerged, err := bt.deleteUnderflow(mtr, parentBranch, childBufPage, 0)

		// THEN
		assert.NoError(t, err)
		assert.True(t, underflow)
		assert.False(t, isLeafMerged)
		assert.Equal(t, 1, parentBranch.numRecords())
		isFree, err := fsp.IsPageFree(mtr, siblingPageId)
		assert.NoError(t, err)
		assert.True(t, isFree)
	})

	t.Run("ブランチノードのアンダーフロー: 転送不可かつマージ不可の場合はアンダーフローを許容する", func(t *testing.T) {
		// GIVEN
		bt, bp := setupBtreeForTest(t)

		childPageId, childBufPage := allocateTestPage(t, bp)
		childBranch := initTestBranchNode(t, bp, childPageId, largeBranchKey(0x10), page.NewId(0, 100), page.NewId(0, 101))
		insertLargeBranchRecords(childBranch, 4, 0x20)

		siblingPageId, _ := allocateTestPage(t, bp)
		siblingBranch := initTestBranchNode(t, bp, siblingPageId, largeBranchKey(0x60), page.NewId(0, 200), page.NewId(0, 201))
		insertLargeBranchRecords(siblingBranch, 4, 0x70)

		parentPageId, _ := allocateTestPage(t, bp)
		parentBranch := initTestBranchNode(t, bp, parentPageId, []byte{0x55}, childPageId, siblingPageId)

		childNumBefore := childBranch.numRecords()
		siblingNumBefore := siblingBranch.numRecords()

		// WHEN
		mtr := buffer.NewMtr(bt.bufferPool)
		defer mtr.UnpinAll()
		underflow, isLeafMerged, err := bt.deleteUnderflow(mtr, parentBranch, childBufPage, 0)

		// THEN
		assert.NoError(t, err)
		assert.False(t, underflow)
		assert.False(t, isLeafMerged)
		assert.Equal(t, childNumBefore, childBranch.numRecords())
		assert.Equal(t, siblingNumBefore, siblingBranch.numRecords())
	})
}

// allocateTestPage はテスト用にページを割り当ててバッファプールに追加する
//   - この経路のページは segment に属さないため FreeSegmentPage の対象にはならない
func allocateTestPage(t *testing.T, bp *buffer.Pool) (page.Id, *buffer.Page) {
	t.Helper()
	allocMtr := buffer.NewWriteMtr(bp, lock.SystemReservedTrxId, newTestRedoBuffer(t))
	pageId, err := fsp.AllocatePage(allocMtr, page.FileId(0))
	assert.NoError(t, err)
	assert.NoError(t, allocMtr.Commit())
	bufPage, err := bp.AddPage(pageId)
	assert.NoError(t, err)
	return pageId, bufPage
}

// allocateTestPageInLeafSegment は tree の leaf segment 経由でページを確保する
//   - onLeafUnderflow が FreeSegmentPage で解放するリーフノード用
func allocateTestPageInLeafSegment(t *testing.T, bp *buffer.Pool, tree *Tree) (page.Id, *buffer.Page) {
	t.Helper()
	allocMtr := buffer.NewWriteMtr(bp, lock.SystemReservedTrxId, newTestRedoBuffer(t))
	pageId, err := fsp.AllocateSegmentPage(allocMtr, tree.MetaPageId().FileId(), tree.leafSegmentHeaderAt())
	assert.NoError(t, err)
	assert.NoError(t, allocMtr.Commit())
	bufPage, err := bp.AddPage(pageId)
	assert.NoError(t, err)
	return pageId, bufPage
}

// allocateTestPageInBranchSegment は tree の非リーフ segment 経由でページを確保する
//   - onBranchUnderflow が FreeSegmentPage で解放するブランチノード用
func allocateTestPageInBranchSegment(t *testing.T, bp *buffer.Pool, tree *Tree) (page.Id, *buffer.Page) {
	t.Helper()
	allocMtr := buffer.NewWriteMtr(bp, lock.SystemReservedTrxId, newTestRedoBuffer(t))
	pageId, err := fsp.AllocateSegmentPage(allocMtr, tree.MetaPageId().FileId(), tree.branchSegmentHeaderAt())
	assert.NoError(t, err)
	assert.NoError(t, allocMtr.Commit())
	bufPage, err := bp.AddPage(pageId)
	assert.NoError(t, err)
	return pageId, bufPage
}

// initTestLeafNode はテスト用の初期化済みリーフノードを作成する
func initTestLeafNode(t *testing.T, bp *buffer.Pool, pageId page.Id) *leafNode {
	t.Helper()
	pg, err := bp.Page(pageId)
	assert.NoError(t, err)
	leaf := newLeafNode(pg)
	leaf.initialize()
	return leaf
}

// initTestBranchNode はテスト用の初期化済みブランチノードを作成する
func initTestBranchNode(
	t *testing.T,
	bp *buffer.Pool,
	pageId page.Id,
	key []byte,
	leftChild, rightChild page.Id,
) *branchNode {
	t.Helper()
	pg, err := bp.Page(pageId)
	assert.NoError(t, err)
	branch := newBranchNode(pg)
	err = branch.initialize(key, leftChild, rightChild)
	assert.NoError(t, err)
	return branch
}

// largeLeafRecord は 900 バイトの nonKey を持つリーフレコードを作成する
func largeLeafRecord(key byte) Record {
	return NewRecord([]byte{}, []byte{key}, make([]byte, 900))
}

// largeBranchKey は 400 バイトのキーを作成する (先頭バイトで識別)
func largeBranchKey(firstByte byte) []byte {
	key := make([]byte, 400)
	key[0] = firstByte
	return key
}

// insertLargeBranchRecords はブランチノードに指定数の大きいレコードを追加する
func insertLargeBranchRecords(bn *branchNode, count int, startKeyByte byte) {
	for i := range count {
		key := largeBranchKey(startKeyByte + byte(i)*0x10)
		record := NewRecord([]byte{}, key, page.NewId(0, page.PageNumber(300+i)).Bytes())
		bn.insert(bn.numRecords(), record)
	}
}
