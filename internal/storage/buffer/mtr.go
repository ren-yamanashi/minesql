package buffer

import (
	"slices"

	"github.com/ren-yamanashi/minesql/internal/storage/page"
)

// pinnedEntry は Mtr が保持中の 1 ページ分の Pin とページラッチの記録
type pinnedEntry struct {
	pageId    page.Id
	mode      LatchMode
	bufPage   *Page
	skipLatch bool // 同一 Mtr 内の再帰取得で実体ラッチを取り直さなかったエントリ
}

// heldLatchEntry は Mtr が保持中の 1 つの任意 RWLatch の記録
type heldLatchEntry struct {
	latch *RWLatch
	mode  LatchMode
}

// Mtr は 1 つの原子的なページ操作で獲得した Pin とラッチをまとめ、操作の完了時に一括解放するスコープ (mini-transaction)
//   - 同じ Mtr 内で同一ページを再取得しても安全 (重複取得はまとめて扱われる)
//   - Pin に紐づかない任意の RWLatch (B+Tree レベルなど) も同スコープで管理する
type Mtr struct {
	pool        *Pool
	pinned      []pinnedEntry
	heldLatches []heldLatchEntry
}

func NewMtr(pool *Pool) *Mtr {
	return &Mtr{pool: pool}
}

// PageForRead は読み込み用のバッファページを取得し、Shared ラッチと Pin をスコープに記録する
func (m *Mtr) PageForRead(pageId page.Id) (*Page, error) {
	bufPage, err := m.pool.PageForRead(pageId)
	if err != nil {
		return nil, err
	}
	skipLatch := false
	if idx, _ := m.findHolder(pageId); idx >= 0 {
		// 既に同 Mtr が何らかの実体ラッチを保持しているなら追加で取らない
		skipLatch = true
	} else {
		bufPage.latch.LockShared()
	}
	m.pinned = append(m.pinned, pinnedEntry{
		pageId: pageId, mode: LatchShared, bufPage: bufPage, skipLatch: skipLatch,
	})
	return bufPage, nil
}

// PageForWrite は書き込み用のバッファページを取得し、Exclusive ラッチと Pin をスコープに記録する
//   - 同一 Mtr 内で S 取得済みのページを X 要求した場合は S を解放して X を取り直す (隙間で他者が X を取りうる)
func (m *Mtr) PageForWrite(pageId page.Id) (*Page, error) {
	bufPage, err := m.pool.PageForWrite(pageId)
	if err != nil {
		return nil, err
	}
	skipLatch := false
	idx, holderMode := m.findHolder(pageId)
	switch {
	case idx < 0:
		bufPage.latch.LockExclusive()
	case holderMode == LatchExclusive:
		skipLatch = true
	case holderMode == LatchShared:
		bufPage.latch.Unlock(LatchShared)
		bufPage.latch.LockExclusive()
		// 既存エントリを X 保持の実体エントリへ昇格させる
		m.pinned[idx].mode = LatchExclusive
		skipLatch = true
	}
	m.pinned = append(m.pinned, pinnedEntry{
		pageId: pageId, mode: LatchExclusive, bufPage: bufPage, skipLatch: skipLatch,
	})
	return bufPage, nil
}

// Unpin は指定ページのラッチと Pin を解放し、スコープの記録から 1 件除外する
//   - LIFO 順で削除するため、再帰取得の最後のエントリから順に解放される
func (m *Mtr) Unpin(pageId page.Id) {
	if entry, ok := m.removePinned(pageId); ok {
		if !entry.skipLatch {
			entry.bufPage.latch.Unlock(entry.mode)
		}
		m.pool.Unpin(pageId)
	}
}

// Detach はラッチを解放しスコープの記録から指定ページを 1 件除外する。Pin は解放しない (走査などへ所有権を移譲する用)
func (m *Mtr) Detach(pageId page.Id) {
	if entry, ok := m.removePinned(pageId); ok {
		if !entry.skipLatch {
			entry.bufPage.latch.Unlock(entry.mode)
		}
	}
}

// UnpinAll はスコープに記録された全ての Pin とラッチを解放する
func (m *Mtr) UnpinAll() {
	// LIFO 順で解放することで、実体ラッチを持つエントリ (最初に取得された) が最後に解放される
	for i := len(m.pinned) - 1; i >= 0; i-- {
		entry := m.pinned[i]
		if !entry.skipLatch {
			entry.bufPage.latch.Unlock(entry.mode)
		}
		m.pool.Unpin(entry.pageId)
	}
	m.pinned = nil
	for i := len(m.heldLatches) - 1; i >= 0; i-- {
		entry := m.heldLatches[i]
		entry.latch.Unlock(entry.mode)
	}
	m.heldLatches = nil
}

// LockShared は任意の RWLatch を Shared で取得し、Mtr スコープに記録する
func (m *Mtr) LockShared(l *RWLatch) {
	l.LockShared()
	m.heldLatches = append(m.heldLatches, heldLatchEntry{latch: l, mode: LatchShared})
}

// LockSharedExclusive は任意の RWLatch を Shared-Exclusive で取得し、Mtr スコープに記録する
func (m *Mtr) LockSharedExclusive(l *RWLatch) {
	l.LockSharedExclusive()
	m.heldLatches = append(m.heldLatches, heldLatchEntry{latch: l, mode: LatchSharedExclusive})
}

// LockExclusive は任意の RWLatch を Exclusive で取得し、Mtr スコープに記録する
func (m *Mtr) LockExclusive(l *RWLatch) {
	l.LockExclusive()
	m.heldLatches = append(m.heldLatches, heldLatchEntry{latch: l, mode: LatchExclusive})
}

// UnlockLatch は指定 RWLatch を 1 件 (LIFO 末尾) 解放する
func (m *Mtr) UnlockLatch(l *RWLatch) {
	for i := len(m.heldLatches) - 1; i >= 0; i-- {
		if m.heldLatches[i].latch == l {
			entry := m.heldLatches[i]
			m.heldLatches = slices.Delete(m.heldLatches, i, i+1)
			entry.latch.Unlock(entry.mode)
			return
		}
	}
}

// PinnedCount はスコープに記録されている Pin の数を返す
func (m *Mtr) PinnedCount() int {
	return len(m.pinned)
}

// HeldLatchCount はスコープに記録されている任意ラッチの数を返す (リーク検出用)
func (m *Mtr) HeldLatchCount() int {
	return len(m.heldLatches)
}

// findHolder は同一ページに対する実体ラッチ保持エントリのインデックスとモードを返す。無ければ (-1, 0) を返す
func (m *Mtr) findHolder(pageId page.Id) (int, LatchMode) {
	for i, entry := range m.pinned {
		if entry.pageId == pageId && !entry.skipLatch {
			return i, entry.mode
		}
	}
	return -1, 0
}

// removePinned はスコープの記録から pageId を 1 件 (LIFO 末尾) 除外する。除外できたエントリを返す
func (m *Mtr) removePinned(pageId page.Id) (pinnedEntry, bool) {
	for i := len(m.pinned) - 1; i >= 0; i-- {
		if m.pinned[i].pageId == pageId {
			entry := m.pinned[i]
			m.pinned = slices.Delete(m.pinned, i, i+1)
			return entry, true
		}
	}
	return pinnedEntry{}, false
}
