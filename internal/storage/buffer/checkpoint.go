package buffer

import "github.com/ren-yamanashi/minesql/internal/storage/log"

// Checkpoint はチェックポイントの実行を管理する
//
// フラッシュリスト内の最小 Page LSN からチェックポイント LSN を算出し、
// 不要な REDO レコードを切り詰める
type Checkpoint struct {
	bp      *BufferPool
	redoLog *log.RedoLog
}

// NewCheckpoint は Checkpoint を生成する
func NewCheckpoint(bp *BufferPool, redoLog *log.RedoLog) *Checkpoint {
	return &Checkpoint{bp: bp, redoLog: redoLog}
}

// Execute はチェックポイントを実行する
//
//  1. フラッシュリスト内の最小 Page LSN を取得
//  2. チェックポイント LSN を算出
//  3. REDO ログヘッダーにチェックポイント LSN を書き込み
//  4. チェックポイント LSN 以前の REDO レコードを切り詰め
func (c *Checkpoint) Execute() error {
	// フラッシュリスト内の最小 Page LSN を取得
	minLSN := c.bp.MinPageLSN()

	// チェックポイント LSN を算出 (ダーティーページが存在しない場合は Flushed LSN をチェックポイント LSN とする)
	checkpointLSN := c.redoLog.FlushedLSN()
	if minLSN > 0 {
		checkpointLSN = log.LSN(minLSN - 1)
	}

	// REDO ログヘッダーにチェックポイント LSN を書き込み
	if err := c.redoLog.SetCheckpointLSN(checkpointLSN); err != nil {
		return err
	}

	// チェックポイント LSN 以前の REDO レコードを切り詰め
	return c.redoLog.TruncateBefore(checkpointLSN)
}
