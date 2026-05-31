package buffer

import (
	"testing"
	"time"

	"github.com/ren-yamanashi/minesql/internal/storage/page"
	"github.com/stretchr/testify/assert"
)

func TestNewMtr(t *testing.T) {
	t.Run("生成直後は記録している Pin が無い", func(t *testing.T) {
		// GIVEN
		bp := NewPool(page.Size*3, nil)

		// WHEN
		mtr := NewMtr(bp)

		// THEN
		assert.Equal(t, 0, mtr.PinnedCount())
	})
}

func TestMtrPageForRead(t *testing.T) {
	t.Run("ページを取得すると Pin が増えスコープに記録される", func(t *testing.T) {
		// GIVEN
		bp := NewPool(page.Size*3, nil)
		pageId := page.NewId(0, 0)
		_, err := bp.AddPage(pageId)
		assert.NoError(t, err)
		mtr := NewMtr(bp)

		// WHEN
		bufPage, err := mtr.PageForRead(pageId)

		// THEN
		assert.NoError(t, err)
		assert.Equal(t, pageId, bufPage.PageId())
		assert.Equal(t, 1, pinCountOf(bp, pageId))
		assert.Equal(t, 1, mtr.PinnedCount())
	})
}

func TestMtrPageForWrite(t *testing.T) {
	t.Run("ページを取得すると Pin が増えスコープに記録される", func(t *testing.T) {
		// GIVEN
		bp := NewPool(page.Size*3, nil)
		pageId := page.NewId(0, 0)
		_, err := bp.AddPage(pageId)
		assert.NoError(t, err)
		mtr := NewMtr(bp)

		// WHEN
		bufPage, err := mtr.PageForWrite(pageId)

		// THEN
		assert.NoError(t, err)
		assert.True(t, bufPage.isDirty)
		assert.Equal(t, 1, pinCountOf(bp, pageId))
		assert.Equal(t, 1, mtr.PinnedCount())
	})

	t.Run("呼び出すたびに更新カウンタが進む", func(t *testing.T) {
		// GIVEN
		bp := NewPool(page.Size*3, nil)
		pageId := page.NewId(0, 0)
		_, err := bp.AddPage(pageId)
		assert.NoError(t, err)
		mtr := NewMtr(bp)

		// WHEN
		bufPage1, err := mtr.PageForWrite(pageId)
		assert.NoError(t, err)
		count1 := bufPage1.modifyCount
		mtr.Unpin(pageId)
		bufPage2, err := mtr.PageForWrite(pageId)
		assert.NoError(t, err)
		count2 := bufPage2.modifyCount

		// THEN
		assert.Equal(t, uint64(1), count1)
		assert.Equal(t, uint64(2), count2)
	})

	t.Run("PageForRead では更新カウンタが進まない", func(t *testing.T) {
		// GIVEN
		bp := NewPool(page.Size*3, nil)
		pageId := page.NewId(0, 0)
		_, err := bp.AddPage(pageId)
		assert.NoError(t, err)
		mtr := NewMtr(bp)

		// WHEN
		bufPage, err := mtr.PageForRead(pageId)

		// THEN
		assert.NoError(t, err)
		assert.Equal(t, uint64(0), bufPage.modifyCount)
	})
}

func TestMtrUnpin(t *testing.T) {
	t.Run("Pin を解放し記録から除外する", func(t *testing.T) {
		// GIVEN
		bp := NewPool(page.Size*3, nil)
		pageId := page.NewId(0, 0)
		_, err := bp.AddPage(pageId)
		assert.NoError(t, err)
		mtr := NewMtr(bp)
		_, err = mtr.PageForRead(pageId)
		assert.NoError(t, err)

		// WHEN
		mtr.Unpin(pageId)

		// THEN
		assert.Equal(t, 0, pinCountOf(bp, pageId))
		assert.Equal(t, 0, mtr.PinnedCount())
	})
}

func TestMtrDetach(t *testing.T) {
	t.Run("記録から除外するが Pin は解放しない", func(t *testing.T) {
		// GIVEN
		bp := NewPool(page.Size*3, nil)
		pageId := page.NewId(0, 0)
		_, err := bp.AddPage(pageId)
		assert.NoError(t, err)
		mtr := NewMtr(bp)
		_, err = mtr.PageForRead(pageId)
		assert.NoError(t, err)

		// WHEN
		mtr.Detach(pageId)

		// THEN
		assert.Equal(t, 1, pinCountOf(bp, pageId))
		assert.Equal(t, 0, mtr.PinnedCount())
	})
}

func TestMtrUnpinAll(t *testing.T) {
	t.Run("記録した全ての Pin を解放する", func(t *testing.T) {
		// GIVEN
		bp := NewPool(page.Size*3, nil)
		pageId1 := page.NewId(0, 0)
		pageId2 := page.NewId(0, 1)
		_, err := bp.AddPage(pageId1)
		assert.NoError(t, err)
		_, err = bp.AddPage(pageId2)
		assert.NoError(t, err)
		mtr := NewMtr(bp)
		_, err = mtr.PageForRead(pageId1)
		assert.NoError(t, err)
		_, err = mtr.PageForWrite(pageId2)
		assert.NoError(t, err)

		// WHEN
		mtr.UnpinAll()

		// THEN
		assert.Equal(t, 0, pinCountOf(bp, pageId1))
		assert.Equal(t, 0, pinCountOf(bp, pageId2))
		assert.Equal(t, 0, mtr.PinnedCount())
	})

	t.Run("Detach したページは解放対象に含まれない", func(t *testing.T) {
		// GIVEN
		bp := NewPool(page.Size*3, nil)
		pageId1 := page.NewId(0, 0)
		pageId2 := page.NewId(0, 1)
		_, err := bp.AddPage(pageId1)
		assert.NoError(t, err)
		_, err = bp.AddPage(pageId2)
		assert.NoError(t, err)
		mtr := NewMtr(bp)
		_, err = mtr.PageForRead(pageId1)
		assert.NoError(t, err)
		_, err = mtr.PageForRead(pageId2)
		assert.NoError(t, err)
		mtr.Detach(pageId2)

		// WHEN
		mtr.UnpinAll()

		// THEN
		assert.Equal(t, 0, pinCountOf(bp, pageId1))
		assert.Equal(t, 1, pinCountOf(bp, pageId2))
	})
}

func TestMtrPinnedCount(t *testing.T) {
	t.Run("複数ページの取得で件数が増える", func(t *testing.T) {
		// GIVEN
		bp := NewPool(page.Size*3, nil)
		pageId1 := page.NewId(0, 0)
		pageId2 := page.NewId(0, 1)
		_, err := bp.AddPage(pageId1)
		assert.NoError(t, err)
		_, err = bp.AddPage(pageId2)
		assert.NoError(t, err)
		mtr := NewMtr(bp)

		// WHEN
		_, err = mtr.PageForRead(pageId1)
		assert.NoError(t, err)
		_, err = mtr.PageForWrite(pageId2)
		assert.NoError(t, err)

		// THEN
		assert.Equal(t, 2, mtr.PinnedCount())
	})

	t.Run("解放すると件数が減る", func(t *testing.T) {
		// GIVEN
		bp := NewPool(page.Size*3, nil)
		pageId1 := page.NewId(0, 0)
		pageId2 := page.NewId(0, 1)
		_, err := bp.AddPage(pageId1)
		assert.NoError(t, err)
		_, err = bp.AddPage(pageId2)
		assert.NoError(t, err)
		mtr := NewMtr(bp)
		_, err = mtr.PageForRead(pageId1)
		assert.NoError(t, err)
		_, err = mtr.PageForRead(pageId2)
		assert.NoError(t, err)

		// WHEN
		mtr.Unpin(pageId1)

		// THEN
		assert.Equal(t, 1, mtr.PinnedCount())
	})
}

func TestMtrLatchRelease(t *testing.T) {
	t.Run("UnpinAll 後は別の Mtr が X ラッチを取得できる", func(t *testing.T) {
		// GIVEN
		bp := NewPool(page.Size*3, nil)
		pageId := page.NewId(0, 0)
		_, err := bp.AddPage(pageId)
		assert.NoError(t, err)
		m1 := NewMtr(bp)
		_, err = m1.PageForWrite(pageId)
		assert.NoError(t, err)

		// WHEN
		m1.UnpinAll()

		// THEN
		m2 := NewMtr(bp)
		acquired := make(chan struct{})
		go func() {
			_, _ = m2.PageForWrite(pageId)
			close(acquired)
		}()
		select {
		case <-acquired:
		case <-time.After(time.Second):
			t.Fatal("UnpinAll 後も X ラッチが残っている")
		}
		m2.UnpinAll()
	})

	t.Run("Unpin 後は別の Mtr が X ラッチを取得できる", func(t *testing.T) {
		// GIVEN
		bp := NewPool(page.Size*3, nil)
		pageId := page.NewId(0, 0)
		_, err := bp.AddPage(pageId)
		assert.NoError(t, err)
		m1 := NewMtr(bp)
		_, err = m1.PageForWrite(pageId)
		assert.NoError(t, err)

		// WHEN
		m1.Unpin(pageId)

		// THEN
		m2 := NewMtr(bp)
		acquired := make(chan struct{})
		go func() {
			_, _ = m2.PageForWrite(pageId)
			close(acquired)
		}()
		select {
		case <-acquired:
		case <-time.After(time.Second):
			t.Fatal("Unpin 後も X ラッチが残っている")
		}
		m2.UnpinAll()
	})

	t.Run("Detach 後は別の Mtr が X ラッチを取得できる", func(t *testing.T) {
		// GIVEN
		bp := NewPool(page.Size*3, nil)
		pageId := page.NewId(0, 0)
		_, err := bp.AddPage(pageId)
		assert.NoError(t, err)
		m1 := NewMtr(bp)
		_, err = m1.PageForRead(pageId)
		assert.NoError(t, err)

		// WHEN
		m1.Detach(pageId)

		// THEN
		m2 := NewMtr(bp)
		acquired := make(chan struct{})
		go func() {
			_, _ = m2.PageForWrite(pageId)
			close(acquired)
		}()
		select {
		case <-acquired:
		case <-time.After(time.Second):
			t.Fatal("Detach 後も S ラッチが残っている")
		}
		m2.UnpinAll()
		bp.Unpin(pageId) // m1.Detach 由来の Pin を解放
	})
}

func TestMtrRecursiveAcquisition(t *testing.T) {
	t.Run("同 Mtr 内の再帰 X 取得はラッチを取り直さない", func(t *testing.T) {
		// GIVEN
		bp := NewPool(page.Size*3, nil)
		pageId := page.NewId(0, 0)
		_, err := bp.AddPage(pageId)
		assert.NoError(t, err)
		mtr := NewMtr(bp)
		bufPage1, err := mtr.PageForWrite(pageId)
		assert.NoError(t, err)
		assert.Equal(t, uint64(1), bufPage1.modifyCount)

		// WHEN: 再帰的に同じページを X 取得
		done := make(chan struct{})
		go func() {
			_, _ = mtr.PageForWrite(pageId)
			close(done)
		}()

		// THEN: ブロックしない (デッドロックしないこと)
		select {
		case <-done:
		case <-time.After(time.Second):
			t.Fatal("再帰 X 取得がデッドロックした")
		}
		assert.Equal(t, uint64(1), bufPage1.modifyCount) // 再帰では更新カウンタが進まない
		assert.Equal(t, 2, mtr.PinnedCount())

		// CLEANUP
		mtr.UnpinAll()
		assert.Equal(t, 0, pinCountOf(bp, pageId))
	})

	t.Run("同 Mtr 内の S 取得後の X 要求は S を解放して X に昇格する", func(t *testing.T) {
		// GIVEN
		bp := NewPool(page.Size*3, nil)
		pageId := page.NewId(0, 0)
		_, err := bp.AddPage(pageId)
		assert.NoError(t, err)
		mtr := NewMtr(bp)
		_, err = mtr.PageForRead(pageId)
		assert.NoError(t, err)

		// WHEN
		done := make(chan struct{})
		go func() {
			_, _ = mtr.PageForWrite(pageId)
			close(done)
		}()

		// THEN
		select {
		case <-done:
		case <-time.After(time.Second):
			t.Fatal("S→X 昇格がデッドロックした")
		}

		// CLEANUP 後に別 Mtr が X を取れること (= 全ラッチ解放されている)
		mtr.UnpinAll()
		m2 := NewMtr(bp)
		acquired := make(chan struct{})
		go func() {
			_, _ = m2.PageForWrite(pageId)
			close(acquired)
		}()
		select {
		case <-acquired:
		case <-time.After(time.Second):
			t.Fatal("UnpinAll 後に X 取得できない (ラッチ漏れ)")
		}
		m2.UnpinAll()
	})

	t.Run("同 Mtr 内の X 取得後の S 要求はスキップされる", func(t *testing.T) {
		// GIVEN
		bp := NewPool(page.Size*3, nil)
		pageId := page.NewId(0, 0)
		_, err := bp.AddPage(pageId)
		assert.NoError(t, err)
		mtr := NewMtr(bp)
		_, err = mtr.PageForWrite(pageId)
		assert.NoError(t, err)

		// WHEN
		done := make(chan struct{})
		go func() {
			_, _ = mtr.PageForRead(pageId)
			close(done)
		}()

		// THEN
		select {
		case <-done:
		case <-time.After(time.Second):
			t.Fatal("X 下の S 取得がデッドロックした")
		}
		mtr.UnpinAll()
	})
}

// pinCountOf は指定ページの現在の pinCount を返す
func pinCountOf(bp *Pool, pageId page.Id) int {
	bufId, ok := bp.pageTable.bufferId(pageId)
	if !ok {
		return -1
	}
	return bp.pages[bufId].pinCount
}
