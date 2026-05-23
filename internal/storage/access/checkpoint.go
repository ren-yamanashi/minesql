package access

import (
	"encoding/binary"

	"github.com/ren-yamanashi/minesql/internal/storage/buffer"
	"github.com/ren-yamanashi/minesql/internal/storage/page"
	"github.com/ren-yamanashi/minesql/internal/storage/redo"
)

// Checkpoint はチェックポイントの実行を管理する
type Checkpoint struct {
	bufferPool *buffer.Pool
	redoLog    *redo.Buffer
}

func NewCheckpoint(bp *buffer.Pool, redo *redo.Buffer) *Checkpoint {
	return &Checkpoint{bufferPool: bp, redoLog: redo}
}

// Execute はチェックポイントを実行する
func (c *Checkpoint) Execute() error {
	minLsn := c.minPageLsn()
	checkpointLsn := c.redoLog.FlushedLsn()
	if minLsn > 0 {
		checkpointLsn = minLsn - 1
	}
	if err := c.redoLog.SetCheckpointLsn(checkpointLsn); err != nil {
		return err
	}
	return c.redoLog.TruncateBefore(checkpointLsn)
}

// minPageLsn はフラッシュリスト内の全ダーティーページの最小 Page LSN を返す
func (c *Checkpoint) minPageLsn() redo.Lsn {
	var minLsn redo.Lsn
	first := true
	c.bufferPool.ForEachDirtyPage(func(pg *page.Page) {
		lsn := redo.Lsn(binary.BigEndian.Uint32(pg.Header))
		if first || lsn < minLsn {
			minLsn = lsn
			first = false
		}
	})
	return minLsn
}
