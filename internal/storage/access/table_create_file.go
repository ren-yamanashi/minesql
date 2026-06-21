package access

import (
	"fmt"
	"path/filepath"

	"github.com/ren-yamanashi/minesql/internal/storage/buffer"
	"github.com/ren-yamanashi/minesql/internal/storage/config"
	"github.com/ren-yamanashi/minesql/internal/storage/dictionary"
	"github.com/ren-yamanashi/minesql/internal/storage/file"
	"github.com/ren-yamanashi/minesql/internal/storage/lock"
	"github.com/ren-yamanashi/minesql/internal/storage/page"
	"github.com/ren-yamanashi/minesql/internal/storage/redo"
	"github.com/ren-yamanashi/minesql/internal/storage/undo"
)

// createTableFile はテーブルのファイルを作成する
//   - FileId 採番と AllocateFileIdUndo 書き込みを同一 mtr で原子的に行う
//   - 物理ファイル作成は mtr Commit 後に実行する
func createTableFile(ct *dictionary.Catalog, bp *buffer.Pool, redoLog *redo.Buffer, tableName string) (page.FileId, error) {
	path := filepath.Join(config.BaseDir, fmt.Sprintf("%s.db", tableName))
	mtr := buffer.NewWriteMtr(bp, lock.DDLReservedTrxId, redoLog)
	fileId, err := ct.AllocateFileId(mtr)
	if err != nil {
		mtr.UnpinAll()
		return 0, err
	}
	undoRecord := undo.NewDDLRecord(
		undo.DDLRecordTypeAllocateFileId,
		undo.NewAllocateFileIdUndoRecord(fileId).Serialize(),
	)
	if err := ct.DDLManager().Append(mtr, undoRecord); err != nil {
		mtr.UnpinAll()
		return 0, err
	}
	if err := mtr.Commit(); err != nil {
		return 0, err
	}
	hp, err := file.NewHeapFile(fileId, path)
	if err != nil {
		return 0, err
	}
	bp.RegisterHeapFile(fileId, hp)
	return fileId, nil
}
