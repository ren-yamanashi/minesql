package buffer

import "minesql/internal/storage/log"

// PageCleaner はダーティーページのフラッシュを管理する
type PageCleaner struct {
	bp              *BufferPool
	redoLog         *log.RedoLog
	redoLogMaxSize  int // REDO ログの最大サイズ (バイト)
	maxDirtyPagePct int // ダーティーページ率の上限 (%)
}

// NewPageCleaner は PageCleaner を生成する
func NewPageCleaner(bp *BufferPool, redoLog *log.RedoLog, redoLogMaxSize int, maxDirtyPagePct int) *PageCleaner {
	return &PageCleaner{
		bp:              bp,
		redoLog:         redoLog,
		redoLogMaxSize:  redoLogMaxSize,
		maxDirtyPagePct: maxDirtyPagePct,
	}
}

// FlushIfNeeded は閾値を超えている場合にダーティーページをフラッシュする
func (pc *PageCleaner) FlushIfNeeded() error {
	if !pc.shouldFlush() {
		return nil
	}

	// フラッシュリストの 1/4 をフラッシュ (最低 1 ページ)
	flushCount := max(pc.bp.FlushList.Size/4, 1)
	return pc.bp.FlushOldestPages(flushCount)
}

// shouldFlush は閾値 (以下のいずれか) を超えているかを判定する
//   - REDO ログサイズが redoLogMaxSize を超えている
//   - ダーティーページ率が maxDirtyPagePct を超えている
func (pc *PageCleaner) shouldFlush() bool {
	if pc.bp.FlushList.Size == 0 {
		return false
	}

	// REDO ログサイズの閾値チェック
	fileSize, err := pc.redoLog.FileSize()
	if err != nil {
		fileSize = 0
	}
	if fileSize+int64(pc.redoLog.BufferSize()) > int64(pc.redoLogMaxSize) {
		return true
	}

	// ダーティーページ率の閾値チェック
	dirtyPct := pc.bp.FlushList.Size * 100 / pc.bp.maxBufferSize
	return dirtyPct > pc.maxDirtyPagePct
}
