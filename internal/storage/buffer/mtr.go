package buffer

import (
	"encoding/binary"
	"fmt"
	"slices"

	"github.com/ren-yamanashi/minesql/internal/storage/lock"
	"github.com/ren-yamanashi/minesql/internal/storage/page"
	"github.com/ren-yamanashi/minesql/internal/storage/redo"
)

// pinnedEntry は Mtr が保持中の 1 ページ分の Pin とページラッチの記録
type pinnedEntry struct {
	pageId      page.Id
	mode        LatchMode
	bufPage     *Page
	skipLatch   bool   // 同一 Mtr 内の再帰取得で実体ラッチを取り直さなかったエントリ
	modifyCount uint64 // X 取得時の更新カウンタ。解放時にこれと比較して変更を検出する
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
	pool          *Pool
	pinned        []pinnedEntry
	heldLatches   []heldLatchEntry
	trxId         lock.TrxId
	redo          *redo.Buffer
	mtrStartLsn   redo.Lsn // この Mtr が最初に記録した MtrStart の LSN (= ダーティ化開始 LSN)。未採番時は 0
	hasLoggedPage bool     // この Mtr で 1 つでもページを Redo 記録したか
	logErr        error    // 記録中に発生した最初のエラー。発生後は記録を行わない
}

func NewMtr(pool *Pool) *Mtr {
	return &Mtr{pool: pool}
}

// NewWriteMtr は書き込み用の Mtr を生成する
//   - 変更ページを Unpin / Commit 時に Redo へ記録し Page LSN をスタンプする
func NewWriteMtr(pool *Pool, trxId lock.TrxId, redoLog *redo.Buffer) *Mtr {
	return &Mtr{pool: pool, trxId: trxId, redo: redoLog}
}

// PageForRead は読み込み用のバッファページを取得し、Shared ラッチと Pin をスコープに記録する
func (m *Mtr) PageForRead(pageId page.Id) (*Page, error) {
	bufPage, err := m.pool.Page(pageId)
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
		modifyCount: bufPage.modifyCount,
	})
	return bufPage, nil
}

// PageForWrite は書き込み用のバッファページを取得し、Exclusive ラッチと Pin をスコープに記録する
//   - 同一 Mtr 内で S 取得済みのページを X 要求した場合は S を解放して X を取り直す (隙間で他者が X を取りうる)
func (m *Mtr) PageForWrite(pageId page.Id) (*Page, error) {
	bufPage, err := m.pool.Page(pageId)
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
		// 既存エントリを X 保持の実体エントリへ昇格させ、X 取得時点の更新カウンタを基準にする
		m.pinned[idx].mode = LatchExclusive
		m.pinned[idx].modifyCount = bufPage.modifyCount
		skipLatch = true
	}
	m.pinned = append(m.pinned, pinnedEntry{
		pageId: pageId, mode: LatchExclusive, bufPage: bufPage, skipLatch: skipLatch,
		modifyCount: bufPage.modifyCount,
	})
	return bufPage, nil
}

// Unpin は指定ページのラッチと Pin を解放し、スコープの記録から 1 件除外する
//   - LIFO 順で削除するため、再帰取得の最後のエントリから順に解放される
//   - ラッチ解放前に、変更済みであれば Redo へ記録し Page LSN をスタンプする
//   - 変更ありの X ラッチを保持中のページを指定すると panic する
func (m *Mtr) Unpin(pageId page.Id) {
	if entry, ok := m.removePinned(pageId); ok {
		m.assertReleasable(entry)
		m.logPageIfModified(entry)
		if !entry.skipLatch {
			entry.bufPage.latch.Unlock(entry.mode)
		}
		m.pool.Unpin(pageId)
	}
}

// Detach はラッチを解放しスコープの記録から指定ページを 1 件除外する。Pin は解放しない (走査などへ所有権を移譲する用)
//   - 変更ありの X ラッチを保持中のページを指定すると panic する
func (m *Mtr) Detach(pageId page.Id) {
	if entry, ok := m.removePinned(pageId); ok {
		m.assertReleasable(entry)
		if !entry.skipLatch {
			entry.bufPage.latch.Unlock(entry.mode)
		}
	}
}

// assertReleasable は変更ありの X ラッチを mtr 途中で解放しようとした場合に panic する
//   - 一括解放経路 (UnpinAll / Commit) は本チェックを経由しないため、 mtr 完了時の解放は許容される
//   - skipLatch なエントリ (= 同一ページの再帰取得) と非 X モードのエントリは検証対象外
func (m *Mtr) assertReleasable(entry pinnedEntry) {
	if entry.skipLatch || entry.mode != LatchExclusive {
		return
	}
	if entry.bufPage.modifyCount == entry.modifyCount {
		return
	}
	panic(fmt.Sprintf("buffer: mtr cannot release modified X-latch in mid-mtr (pageId=%v); release via mtr.Commit or mtr.UnpinAll", entry.pageId))
}

// UnpinAll はスコープに記録された全ての Pin とラッチを解放する
//   - 各ページはラッチ解放前に、変更済みであれば Redo へ記録し Page LSN をスタンプする
//   - MtrEnd は書かないため、記録途中の Mtr はクラッシュリカバリ時に破棄される
func (m *Mtr) UnpinAll() {
	// LIFO 順で記録・解放することで、実体ラッチを持つエントリ (最初に取得された) が最後に解放される
	for i := len(m.pinned) - 1; i >= 0; i-- {
		m.logPageIfModified(m.pinned[i])
	}
	m.releaseAll()
}

// Commit は保持中の変更ページを Redo へ記録し、1 つでも記録していれば MtrEnd を書いてから全ラッチ・Pin を解放する
//   - 記録中にエラーが発生した場合は MtrEnd を書かず、そのエラーを返す
//   - 記録は LIFO 順で行うため、Undo ページ切替時の「新ページの実体 → 旧ページのリンク」順序が保たれる
func (m *Mtr) Commit() error {
	for i := len(m.pinned) - 1; i >= 0; i-- {
		m.logPageIfModified(m.pinned[i])
	}
	if m.hasLoggedPage && m.logErr == nil {
		if _, err := m.redo.AppendMtrEnd(m.trxId); err != nil {
			m.logErr = err
		}
	}
	err := m.logErr
	m.releaseAll()
	return err
}

// releaseAll は記録を行わず、スコープの全ラッチ・Pin を LIFO 順で解放する
func (m *Mtr) releaseAll() {
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

// logPageIfModified は X ラッチ保持中の変更済みページを Redo へ記録し、Page LSN をスタンプする
//   - 読み取り専用 Mtr (redo 未設定)・実体ラッチ非保持・X 以外・未変更ページは記録しない
//   - スタンプはページ全体コピーの前に行うため、コピーにそのレコード自身の LSN が含まれる
//   - MtrStart 採番直後に bufPage.markDirtyFromLsn(mtrStartLsn) を呼び、ダーティ化開始 LSN を記録する
func (m *Mtr) logPageIfModified(entry pinnedEntry) {
	if m.redo == nil || m.logErr != nil || entry.skipLatch || entry.mode != LatchExclusive {
		return
	}
	bufPage := entry.bufPage
	if bufPage.modifyCount == entry.modifyCount {
		return
	}
	if !m.hasLoggedPage {
		lsn, err := m.redo.AppendMtrStart(m.trxId)
		if err != nil {
			m.logErr = err
			return
		}
		m.mtrStartLsn = lsn
		m.hasLoggedPage = true
	}
	bufPage.markDirtyFromLsn(m.mtrStartLsn)
	_, err := m.redo.AppendPageWrite(m.trxId, entry.pageId, bufPage.data, func(lsn redo.Lsn) {
		var b [page.HeaderSize]byte
		binary.BigEndian.PutUint32(b[:], uint32(lsn))
		bufPage.WriteHeaderAt(0, b[:])
	})
	if err != nil {
		m.logErr = err
	}
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
