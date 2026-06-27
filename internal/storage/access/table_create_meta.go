package access

import (
	"github.com/ren-yamanashi/minesql/internal/storage/buffer"
	"github.com/ren-yamanashi/minesql/internal/storage/dictionary"
	"github.com/ren-yamanashi/minesql/internal/storage/page"
	"github.com/ren-yamanashi/minesql/internal/storage/undo"
)

// registerTableMeta はテーブルメタ・インデックスメタ (プライマリ)・カラムメタをカタログに登録する
//   - mtr: 登録の書き込みを記録する Mtr。Commit は呼び出し側
//   - 各 Insert 直後に対応する MetaInsertUndo を Append する
func registerTableMeta(
	ddlTrx *Transaction,
	mtr *buffer.Mtr,
	fileId page.FileId,
	pi *primaryIndex,
	input CreateTableInput,
) error {
	ct := ddlTrx.catalog
	// テーブルメタ
	tableRecord := dictionary.NewTableMetaRecord(input.TableName, pi.tree.MetaPageId(), len(input.ColNames))
	tableKey := tableRecord.Encode().Key()
	if err := ct.TableMeta().Insert(mtr, tableRecord); err != nil {
		return err
	}
	if err := appendMetaInsertUndo(ddlTrx, mtr, undo.MetaTableTypeTable, tableKey); err != nil {
		return err
	}

	// インデックスメタ
	indexId, err := ct.AllocateIndexId(mtr)
	if err != nil {
		return err
	}
	indexRecord := dictionary.NewIndexMetaRecord(
		fileId,
		indexId,
		dictionary.PrimaryIndexName,
		dictionary.IndexTypePrimary,
		input.PkCount,
		pi.tree.MetaPageId(),
	)
	indexKey := indexRecord.Encode().Key()
	if err := ct.IndexMeta().Insert(mtr, indexRecord); err != nil {
		return err
	}
	if err := appendMetaInsertUndo(ddlTrx, mtr, undo.MetaTableTypeIndex, indexKey); err != nil {
		return err
	}

	// カラムメタ
	for i, col := range input.ColNames {
		colRecord := dictionary.NewColumnMetaRecord(fileId, col, i)
		colKey := colRecord.Encode().Key()
		if err := ct.ColumnMeta().Insert(mtr, colRecord); err != nil {
			return err
		}
		if err := appendMetaInsertUndo(ddlTrx, mtr, undo.MetaTableTypeColumn, colKey); err != nil {
			return err
		}
	}
	return nil
}
