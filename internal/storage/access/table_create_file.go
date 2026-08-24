package access

import (
	"fmt"
	"path/filepath"

	"github.com/ren-yamanashi/minesql/internal/storage/config"
	"github.com/ren-yamanashi/minesql/internal/storage/file"
	"github.com/ren-yamanashi/minesql/internal/storage/fsp"
	"github.com/ren-yamanashi/minesql/internal/storage/page"
	"github.com/ren-yamanashi/minesql/internal/storage/undo"
)

// createTableFile はテーブルのファイルを作成する
//   - FileId 採番と AllocateFileIdUndo 書き込みを同一 mtr で原子的に行う
//   - 物理ファイル作成は mtr Commit 後に実行する
//   - エラー時も mtr は commit される (既に書き込まれた分は DDL Undo 経由で取り消す)
func createTableFile(ddlTrx *Transaction, tableName string) (page.FileId, error) {
	path := filepath.Join(config.BaseDir, fmt.Sprintf("%s.db", tableName))
	mtr := ddlTrx.NewMtr()
	fileId, allocErr := ddlTrx.catalog.AllocateFileId(mtr)
	if allocErr == nil {
		undoRecord := undo.NewDDLRecord(
			undo.DDLRecordTypeAllocateFileId,
			undo.NewAllocateFileIdUndoRecord(fileId).Serialize(),
		)
		allocErr = ddlTrx.DDLManager().Append(mtr, undoRecord)
	}
	if commitErr := mtr.Commit(); commitErr != nil && allocErr == nil {
		allocErr = commitErr
	}
	if allocErr != nil {
		return 0, allocErr
	}
	hp, err := file.NewHeapFile(path)
	if err != nil {
		return 0, err
	}
	ddlTrx.bufferPool.RegisterHeapFile(fileId, hp)
	initMtr := ddlTrx.NewMtr()
	initErr := fsp.InitHeader(initMtr, fileId)
	if commitErr := initMtr.Commit(); commitErr != nil && initErr == nil {
		initErr = commitErr
	}
	if initErr != nil {
		return 0, initErr
	}
	return fileId, nil
}
