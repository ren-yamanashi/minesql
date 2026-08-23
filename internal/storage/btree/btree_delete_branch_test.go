package btree

import (
	"testing"

	"github.com/ren-yamanashi/minesql/internal/storage/buffer"
	"github.com/ren-yamanashi/minesql/internal/storage/fsp"
	"github.com/ren-yamanashi/minesql/internal/storage/page"
	"github.com/stretchr/testify/assert"
)

func TestOnBranchUnderflow(t *testing.T) {
	t.Run("右の兄弟から転送: 親の境界キーを子に下ろし兄弟の末尾キーを親に上げる", func(t *testing.T) {
		// GIVEN
		bt, bp := setupBtreeForTest(t)
		childPageId, childBufPage := allocateTestPage(t, bp)
		childBranch := initTestBranchNode(t, bp, childPageId, largeBranchKey(0x10), page.NewId(0, 100), page.NewId(0, 101))
		insertLargeBranchRecords(childBranch, 3, 0x20)
		siblingPageId, siblingBufPage := allocateTestPage(t, bp)
		siblingBranch := initTestBranchNode(t, bp, siblingPageId, largeBranchKey(0x60), page.NewId(0, 200), page.NewId(0, 201))
		insertLargeBranchRecords(siblingBranch, 5, 0x70)
		parentPageId, _ := allocateTestPage(t, bp)
		parentBranch := initTestBranchNode(t, bp, parentPageId, []byte{0x55}, childPageId, siblingPageId)
		childNumBefore := childBranch.numRecords()
		siblingNumBefore := siblingBranch.numRecords()

		// WHEN
		mtr := buffer.NewMtr(bt.bufferPool)
		defer mtr.UnpinAll()
		sibling := siblingInfo{pageId: siblingPageId, bufferPage: siblingBufPage, isLeft: false}
		underflow, err := bt.onBranchUnderflow(mtr, parentBranch, childBufPage, sibling, 0)

		// THEN
		assert.NoError(t, err)
		assert.False(t, underflow)
		assert.Equal(t, childNumBefore+1, childBranch.numRecords())
		assert.Equal(t, siblingNumBefore-1, siblingBranch.numRecords())
	})

	t.Run("左の兄弟とマージ: 消滅する子ノードのページが解放される", func(t *testing.T) {
		// GIVEN
		bt, bp := setupBtreeForTest(t)
		siblingPageId, siblingBufPage := allocateTestPageInBranchSegment(t, bp, bt)
		siblingBranch := initTestBranchNode(t, bp, siblingPageId, largeBranchKey(0x10), page.NewId(0, 200), page.NewId(0, 201))
		childPageId, childBufPage := allocateTestPageInBranchSegment(t, bp, bt)
		initTestBranchNode(t, bp, childPageId, largeBranchKey(0x70), page.NewId(0, 100), page.NewId(0, 101))
		parentPageId, _ := allocateTestPage(t, bp)
		parentBranch := initTestBranchNode(t, bp, parentPageId, []byte{0x60}, siblingPageId, childPageId)

		// WHEN (childSlotNum = NumRecords = 1 → 左の兄弟が選ばれる)
		mtr := buffer.NewMtr(bt.bufferPool)
		defer mtr.UnpinAll()
		sibling := siblingInfo{pageId: siblingPageId, bufferPage: siblingBufPage, isLeft: true}
		underflow, err := bt.onBranchUnderflow(mtr, parentBranch, childBufPage, sibling, 1)

		// THEN
		assert.NoError(t, err)
		assert.True(t, underflow)
		assert.Equal(t, 0, parentBranch.numRecords())
		assert.Equal(t, siblingBufPage.PageId(), parentBranch.rightChildPageId())
		assert.Equal(t, 3, siblingBranch.numRecords())
		assert.Equal(t, largeBranchKey(0x10), siblingBranch.record(0).Key())
		assert.Equal(t, page.NewId(0, 200).Bytes(), siblingBranch.record(0).NonKey())
		assert.Equal(t, []byte{0x60}, siblingBranch.record(1).Key())
		assert.Equal(t, page.NewId(0, 201).Bytes(), siblingBranch.record(1).NonKey())
		assert.Equal(t, largeBranchKey(0x70), siblingBranch.record(2).Key())
		assert.Equal(t, page.NewId(0, 100).Bytes(), siblingBranch.record(2).NonKey())
		assert.Equal(t, page.NewId(0, 101), siblingBranch.rightChildPageId())
		isFree, err := fsp.IsPageFree(mtr, childPageId)
		assert.NoError(t, err)
		assert.True(t, isFree)
	})

	t.Run("転送不可かつマージ不可の場合はアンダーフローを許容する", func(t *testing.T) {
		// GIVEN
		bt, bp := setupBtreeForTest(t)
		childPageId, childBufPage := allocateTestPage(t, bp)
		childBranch := initTestBranchNode(t, bp, childPageId, largeBranchKey(0x10), page.NewId(0, 100), page.NewId(0, 101))
		insertLargeBranchRecords(childBranch, 4, 0x20)
		siblingPageId, siblingBufPage := allocateTestPage(t, bp)
		siblingBranch := initTestBranchNode(t, bp, siblingPageId, largeBranchKey(0x60), page.NewId(0, 200), page.NewId(0, 201))
		insertLargeBranchRecords(siblingBranch, 4, 0x70)
		parentPageId, _ := allocateTestPage(t, bp)
		parentBranch := initTestBranchNode(t, bp, parentPageId, []byte{0x55}, childPageId, siblingPageId)
		childNumBefore := childBranch.numRecords()
		siblingNumBefore := siblingBranch.numRecords()

		// WHEN
		mtr := buffer.NewMtr(bt.bufferPool)
		defer mtr.UnpinAll()
		sibling := siblingInfo{pageId: siblingPageId, bufferPage: siblingBufPage, isLeft: false}
		underflow, err := bt.onBranchUnderflow(mtr, parentBranch, childBufPage, sibling, 0)

		// THEN
		assert.NoError(t, err)
		assert.False(t, underflow)
		assert.Equal(t, childNumBefore, childBranch.numRecords())
		assert.Equal(t, siblingNumBefore, siblingBranch.numRecords())
	})
}
