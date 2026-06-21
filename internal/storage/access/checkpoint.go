package access

import (
	"github.com/ren-yamanashi/minesql/internal/storage/buffer"
	"github.com/ren-yamanashi/minesql/internal/storage/redo"
)

type Checkpoint struct {
	bufferPool *buffer.Pool
	redoLog    *redo.Buffer
	trxMgr     *TrxManager
}

func NewCheckpoint(bp *buffer.Pool, redo *redo.Buffer, trxMgr *TrxManager) *Checkpoint {
	return &Checkpoint{bufferPool: bp, redoLog: redo, trxMgr: trxMgr}
}

func (c *Checkpoint) Execute() error {
	if err := c.trxMgr.PersistNextTrxIdToCatalog(); err != nil {
		return err
	}

	minLsn := c.minOldestModificationLsn()
	checkpointLsn := c.redoLog.FlushedLsn()
	if minLsn > 0 {
		checkpointLsn = minLsn - 1
	}
	if err := c.redoLog.SetCheckpointLsn(checkpointLsn); err != nil {
		return err
	}
	return c.redoLog.TruncateBefore(checkpointLsn)
}

// minOldestModificationLsn はフラッシュリスト内の全ダーティーページの「最初にダーティ化した時の LSN」の最小を返す
func (c *Checkpoint) minOldestModificationLsn() redo.Lsn {
	var minLsn redo.Lsn
	first := true
	c.bufferPool.ForEachDirtyOldestLsn(func(lsn redo.Lsn) {
		if first || lsn < minLsn {
			minLsn = lsn
			first = false
		}
	})
	return minLsn
}
