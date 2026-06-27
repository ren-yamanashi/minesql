package access

import (
	"github.com/ren-yamanashi/minesql/internal/storage/buffer"
	"github.com/ren-yamanashi/minesql/internal/storage/lock"
	"github.com/ren-yamanashi/minesql/internal/storage/redo"
)

// newBootstrapMtr は新規 DB 作成時の catalog / undo 初期化専用の mtr を生成する
//   - TrxManager 未生成の段階で必要なため、 ここでのみ lock.SystemReservedTrxId を直書きで使う
//   - 呼び出し側で Commit / UnpinAll を行う
func newBootstrapMtr(bp *buffer.Pool, redoLog *redo.Buffer) *buffer.Mtr {
	return buffer.NewWriteMtr(bp, lock.SystemReservedTrxId, redoLog)
}
