package access

import (
	"fmt"
	"path/filepath"

	"github.com/ren-yamanashi/minesql/internal/storage/config"
	"github.com/ren-yamanashi/minesql/internal/storage/file"
	"github.com/ren-yamanashi/minesql/internal/storage/page"
	"github.com/ren-yamanashi/minesql/internal/storage/undo"
)

// createTableFile はテーブルのファイルを作成する
//   - FileId 採番と AllocateFileIdUndo 書き込みを同一 mtr で原子的に行う
//   - 物理ファイル作成は mtr Commit 後に実行する
func createTableFile(ddlTrx *Transaction, tableName string) (page.FileId, error) {
	path := filepath.Join(config.BaseDir, fmt.Sprintf("%s.db", tableName))
	mtr := ddlTrx.NewMtr()
	fileId, err := ddlTrx.catalog.AllocateFileId(mtr)
	if err != nil {
		mtr.UnpinAll()
		return 0, err
	}
	undoRecord := undo.NewDDLRecord(
		undo.DDLRecordTypeAllocateFileId,
		undo.NewAllocateFileIdUndoRecord(fileId).Serialize(),
	)
	if err := ddlTrx.DDLManager().Append(mtr, undoRecord); err != nil {
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
	ddlTrx.bufferPool.RegisterHeapFile(fileId, hp)
	return fileId, nil
}
