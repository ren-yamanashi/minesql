package buffer

import (
	"slices"

	"github.com/ren-yamanashi/minesql/internal/storage/page"
)

// Mtr は 1 つの原子的なページ操作で獲得した Pin をまとめ、操作の完了時に一括解放するスコープ (mini-transaction)
type Mtr struct {
	pool   *Pool
	pinned []page.Id
}

func NewMtr(pool *Pool) *Mtr {
	return &Mtr{pool: pool}
}

// PageForRead は読み込み用のバッファページを取得し、Pin をスコープに記録する
func (m *Mtr) PageForRead(pageId page.Id) (*Page, error) {
	bufPage, err := m.pool.PageForRead(pageId)
	if err != nil {
		return nil, err
	}
	m.pinned = append(m.pinned, pageId)
	return bufPage, nil
}

// PageForWrite は書き込み用のバッファページを取得し、Pin をスコープに記録する
func (m *Mtr) PageForWrite(pageId page.Id) (*Page, error) {
	bufPage, err := m.pool.PageForWrite(pageId)
	if err != nil {
		return nil, err
	}
	m.pinned = append(m.pinned, pageId)
	return bufPage, nil
}

// Unpin は指定ページの Pin を解放し、スコープの記録から 1 件除外する
func (m *Mtr) Unpin(pageId page.Id) {
	if m.removePinned(pageId) {
		m.pool.Unpin(pageId)
	}
}

// Detach はスコープの記録から指定ページを 1 件除外する。Pin は解放しない (走査などへ所有権を移譲する用)
func (m *Mtr) Detach(pageId page.Id) {
	m.removePinned(pageId)
}

// UnpinAll はスコープに記録された全ての Pin を解放する
func (m *Mtr) UnpinAll() {
	for _, pageId := range m.pinned {
		m.pool.Unpin(pageId)
	}
	m.pinned = nil
}

// PinnedCount はスコープに記録されている Pin の数を返す
func (m *Mtr) PinnedCount() int {
	return len(m.pinned)
}

// removePinned はスコープの記録から pageId を 1 件除外する。除外できたら true を返す
func (m *Mtr) removePinned(pageId page.Id) bool {
	idx := slices.Index(m.pinned, pageId)
	if idx < 0 {
		return false
	}
	m.pinned = slices.Delete(m.pinned, idx, idx+1)
	return true
}
