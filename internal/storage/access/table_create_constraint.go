package access

import (
	"github.com/ren-yamanashi/minesql/internal/storage/buffer"
	"github.com/ren-yamanashi/minesql/internal/storage/dictionary"
	"github.com/ren-yamanashi/minesql/internal/storage/page"
	"github.com/ren-yamanashi/minesql/internal/storage/undo"
)

// createConstraints は制約をカタログに登録する
//   - mtr: 登録の書き込みを記録する Mtr。Commit は呼び出し側
//   - 各 Insert 直後に対応する MetaInsertUndo を Append する
func createConstraints(
	ddlTrx *Transaction,
	mtr *buffer.Mtr,
	fileId page.FileId,
	inputs []CreateConstraintInput,
) error {
	ct := ddlTrx.catalog
	bp := ddlTrx.bufferPool
	for _, input := range inputs {
		refTable, err := fetchTable(ct, bp, input.ReferenceTableName)
		if err != nil {
			return err
		}
		constraintRecord := dictionary.NewConstraintMetaRecord(
			fileId,
			input.ColumnName,
			input.ConstraintName,
			refTable.MetaPageId().FileId(),
			input.ReferenceColumnName,
		)
		constraintKey := constraintRecord.Encode().Key()
		if err := ct.ConstraintMeta().Insert(mtr, constraintRecord); err != nil {
			return err
		}
		if err := appendMetaInsertUndo(ddlTrx, mtr, undo.MetaTableTypeConstraint, constraintKey); err != nil {
			return err
		}
	}
	return nil
}
