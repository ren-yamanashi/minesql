package access

import (
	"fmt"
	"path/filepath"

	"github.com/ren-yamanashi/minesql/internal/storage/btree"
	"github.com/ren-yamanashi/minesql/internal/storage/buffer"
	"github.com/ren-yamanashi/minesql/internal/storage/config"
	"github.com/ren-yamanashi/minesql/internal/storage/dictionary"
	"github.com/ren-yamanashi/minesql/internal/storage/file"
	"github.com/ren-yamanashi/minesql/internal/storage/lock"
	"github.com/ren-yamanashi/minesql/internal/storage/page"
	"github.com/ren-yamanashi/minesql/internal/storage/redo"
	"github.com/ren-yamanashi/minesql/internal/storage/undo"
)

type CreateConstraintInput struct {
	ColumnName          string // 制約のあるカラム名
	ConstraintName      string // 制約名
	ReferenceTableName  string // 制約により参照されるテーブル名
	ReferenceColumnName string // 制約により参照されるカラム名 (構成順通り)
}

type CreateIndexInput struct {
	IndexName string               // インデックス名
	ColNames  []string             // インデックスを構成するカラム名 (構成順通り)
	IndexType dictionary.IndexType // インデックス種類
}

type CreateTableInput struct {
	TableName   string                  // テーブル名
	ColNames    []string                // カラム名のリスト
	PkCount     int                     // プライマリキーのカラム数
	Indexes     []CreateIndexInput      // インデックス
	Constraints []CreateConstraintInput // 制約
}

// CreateTable はテーブルを新規作成する
//   - tm: DDL Transaction を払い出すための TrxManager。 配下の catalog / bufferPool / redoLog / lockMgr / undoLog を共有する
//   - 各 step は DDLReservedTrxId 配下の mtr で「データ変更 + DDL Undo 書き込み」を 1 つの mtr で原子的に記録する
//   - 途中失敗時は defer 経由で tm.Rollback(ddlTrx) を呼び、 DDL Undo を逆順適用して完全クリーンアップする
func CreateTable(tm *TrxManager, input CreateTableInput) (table *Table, err error) {
	ddlTrx := tm.BeginDDL()
	defer func() {
		if err != nil {
			_ = tm.Rollback(ddlTrx)
		}
	}()

	bp := tm.bufferPool
	redoLog := tm.redoLog
	lockMgr := tm.lock
	undoLog := tm.undoLog
	ct := tm.catalog

	// ファイル作成 + AllocateFileIdUndo
	fileId, err := createTableFile(ct, bp, redoLog, input.TableName)
	if err != nil {
		return nil, err
	}

	// プライマリインデックス作成 + CreateBTreeUndo
	pi, err := createPrimaryIndex(ct, bp, fileId, input.PkCount, lockMgr, undoLog, redoLog)
	if err != nil {
		return nil, err
	}

	// テーブルメタ・インデックスメタ・カラムメタの登録 + MetaInsertUndo
	registerMtr := buffer.NewWriteMtr(bp, lock.DDLReservedTrxId, redoLog)
	if err = registerTableMeta(registerMtr, ct, fileId, pi, input); err != nil {
		registerMtr.UnpinAll()
		return nil, err
	}
	if err = registerMtr.Commit(); err != nil {
		return nil, err
	}

	// セカンダリインデックス作成 + 各種 DDL Undo (= 各セカンダリ 1 mtr で atomic)
	sis, err := createSecondaryIndexes(ct, bp, fileId, pi.tree, lockMgr, undoLog, redoLog, input.Indexes)
	if err != nil {
		return nil, err
	}

	// 制約登録 + MetaInsertUndo
	constraintsMtr := buffer.NewWriteMtr(bp, lock.DDLReservedTrxId, redoLog)
	if err = createConstraints(constraintsMtr, ct, bp, fileId, input.Constraints); err != nil {
		constraintsMtr.UnpinAll()
		return nil, err
	}
	if err = constraintsMtr.Commit(); err != nil {
		return nil, err
	}

	if err = tm.Commit(ddlTrx); err != nil {
		return nil, err
	}

	table = &Table{
		primaryIndex:     pi,
		secondaryIndexes: sis,
		catalog:          ct,
		undoLog:          undoLog,
		lock:             lockMgr,
		bufferPool:       bp,
		redoLog:          redoLog,
	}
	return table, nil
}

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

// registerTableMeta はテーブルメタ・インデックスメタ (プライマリ)・カラムメタをカタログに登録する
//   - mtr: 登録の書き込みを記録する Mtr。Commit は呼び出し側
//   - 各 Insert 直後に対応する MetaInsertUndo を Append する
func registerTableMeta(
	mtr *buffer.Mtr,
	ct *dictionary.Catalog,
	fileId page.FileId,
	pi *primaryIndex,
	input CreateTableInput,
) error {
	// テーブルメタ
	tableRecord := dictionary.NewTableMetaRecord(input.TableName, pi.tree.MetaPageId(), len(input.ColNames))
	tableKey := tableRecord.Encode().Key()
	if err := ct.TableMeta().Insert(mtr, tableRecord); err != nil {
		return err
	}
	if err := appendMetaInsertUndo(mtr, ct, undo.MetaTableTypeTable, tableKey); err != nil {
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
	if err := appendMetaInsertUndo(mtr, ct, undo.MetaTableTypeIndex, indexKey); err != nil {
		return err
	}

	// カラムメタ
	for i, col := range input.ColNames {
		colRecord := dictionary.NewColumnMetaRecord(fileId, col, i)
		colKey := colRecord.Encode().Key()
		if err := ct.ColumnMeta().Insert(mtr, colRecord); err != nil {
			return err
		}
		if err := appendMetaInsertUndo(mtr, ct, undo.MetaTableTypeColumn, colKey); err != nil {
			return err
		}
	}
	return nil
}

// createSecondaryIndexes はセカンダリインデックスを作成する
//   - 各セカンダリインデックスごとに 1 mtr で「B+Tree 作成 + CreateBTreeUndo + IndexMeta Insert + MetaInsertUndo + 各 IndexKeyColumnMeta Insert + MetaInsertUndo」を原子的に記録する
//   - 途中失敗時はその mtr を UnpinAll で破棄するため、 そのセカンダリの中間状態はメモリ上から消える (既に Commit 済みのセカンダリは DDL Undo 経由で取り消す)
func createSecondaryIndexes(
	ct *dictionary.Catalog,
	bp *buffer.Pool,
	fileId page.FileId,
	pt *btree.Tree,
	lockMgr *lock.Manager,
	undoLog *undo.Manager,
	redoLog *redo.Buffer,
	inputs []CreateIndexInput,
) ([]*secondaryIndex, error) {
	indexes := make([]*secondaryIndex, 0, len(inputs))
	for _, input := range inputs {
		index, err := buildOneSecondaryIndex(ct, bp, fileId, pt, lockMgr, undoLog, redoLog, input)
		if err != nil {
			return nil, err
		}
		indexes = append(indexes, index)
	}
	return indexes, nil
}

// buildOneSecondaryIndex はセカンダリインデックス 1 件を 1 mtr で作成する
//   - B+Tree 作成、 CreateBTreeUndo、 IndexMeta Insert + MetaInsertUndo、 各 IndexKeyColumnMeta Insert + MetaInsertUndo を全て同一 mtr で行う
func buildOneSecondaryIndex(
	ct *dictionary.Catalog,
	bp *buffer.Pool,
	fileId page.FileId,
	pt *btree.Tree,
	lockMgr *lock.Manager,
	undoLog *undo.Manager,
	redoLog *redo.Buffer,
	input CreateIndexInput,
) (*secondaryIndex, error) {
	mtr := buffer.NewWriteMtr(bp, lock.DDLReservedTrxId, redoLog)

	indexId, err := ct.AllocateIndexId(mtr)
	if err != nil {
		mtr.UnpinAll()
		return nil, err
	}

	index, err := createSecondaryIndex(mtr, ct, bp, createSecondaryIndexInput{
		FileId:      fileId,
		PrimaryTree: pt,
		IndexId:     indexId,
		IndexName:   input.IndexName,
		Unique:      input.IndexType == dictionary.IndexTypeUnique,
		Lock:        lockMgr,
		UndoLog:     undoLog,
	})
	if err != nil {
		mtr.UnpinAll()
		return nil, err
	}

	createBTreeUndo := undo.NewDDLRecord(
		undo.DDLRecordTypeCreateBTree,
		undo.NewCreateBTreeUndoRecord(index.tree.MetaPageId()).Serialize(),
	)
	if err := ct.DDLManager().Append(mtr, createBTreeUndo); err != nil {
		mtr.UnpinAll()
		return nil, err
	}

	indexRecord := dictionary.NewIndexMetaRecord(
		fileId,
		indexId,
		input.IndexName,
		input.IndexType,
		len(input.ColNames),
		index.tree.MetaPageId(),
	)
	indexKey := indexRecord.Encode().Key()
	if err := ct.IndexMeta().Insert(mtr, indexRecord); err != nil {
		mtr.UnpinAll()
		return nil, err
	}
	if err := appendMetaInsertUndo(mtr, ct, undo.MetaTableTypeIndex, indexKey); err != nil {
		mtr.UnpinAll()
		return nil, err
	}

	for i, keyCol := range input.ColNames {
		keyColRecord := dictionary.NewIndexKeyColumnMetaRecord(indexId, keyCol, i)
		keyColKey := keyColRecord.Encode().Key()
		if err := ct.IndexKeyColumnMeta().Insert(mtr, keyColRecord); err != nil {
			mtr.UnpinAll()
			return nil, err
		}
		if err := appendMetaInsertUndo(mtr, ct, undo.MetaTableTypeIndexKeyColumn, keyColKey); err != nil {
			mtr.UnpinAll()
			return nil, err
		}
	}

	if err := mtr.Commit(); err != nil {
		return nil, err
	}
	return index, nil
}

// createConstraints は制約をカタログに登録する
//   - mtr: 登録の書き込みを記録する Mtr。Commit は呼び出し側
//   - 各 Insert 直後に対応する MetaInsertUndo を Append する
func createConstraints(mtr *buffer.Mtr, ct *dictionary.Catalog, bp *buffer.Pool, fileId page.FileId, inputs []CreateConstraintInput) error {
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
		if err := appendMetaInsertUndo(mtr, ct, undo.MetaTableTypeConstraint, constraintKey); err != nil {
			return err
		}
	}
	return nil
}

// appendMetaInsertUndo はカタログ Meta テーブルへの 1 件の Insert を取り消すための MetaInsertUndo を Append する
func appendMetaInsertUndo(mtr *buffer.Mtr, ct *dictionary.Catalog, metaTableType undo.MetaTableType, key []byte) error {
	record := undo.NewDDLRecord(
		undo.DDLRecordTypeMetaInsert,
		undo.NewMetaInsertUndoRecord(metaTableType, key).Serialize(),
	)
	return ct.DDLManager().Append(mtr, record)
}
