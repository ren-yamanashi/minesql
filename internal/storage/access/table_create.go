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
func CreateTable(
	bp *buffer.Pool,
	undoLog *undo.Manager,
	lockMgr *lock.Manager,
	redoLog *redo.Buffer,
	input CreateTableInput,
) (*Table, error) {
	ct, err := dictionary.NewCatalog(bp, redoLog)
	if err != nil {
		return nil, err
	}

	// ファイル作成
	fileId, err := createTableFile(ct, bp, redoLog, input.TableName)
	if err != nil {
		return nil, err
	}

	// プライマリインデックス作成
	pi, err := createPrimaryIndex(ct, bp, fileId, input.PkCount, lockMgr, undoLog, redoLog)
	if err != nil {
		return nil, err
	}

	// テーブルメタ・カラムメタをカタログに登録
	registerMtr := buffer.NewWriteMtr(bp, lock.SystemReservedTrxId, redoLog)
	if err := registerTableMeta(registerMtr, ct, fileId, pi, input); err != nil {
		registerMtr.UnpinAll()
		return nil, err
	}
	if err := registerMtr.Commit(); err != nil {
		return nil, err
	}

	// セカンダリインデックス作成
	secondaryMtr := buffer.NewWriteMtr(bp, lock.SystemReservedTrxId, redoLog)
	sis, err := createSecondaryIndexes(secondaryMtr, ct, bp, fileId, pi.tree, lockMgr, undoLog, redoLog, input.Indexes)
	if err != nil {
		secondaryMtr.UnpinAll()
		return nil, err
	}
	if err := secondaryMtr.Commit(); err != nil {
		return nil, err
	}

	// 制約作成
	constraintsMtr := buffer.NewWriteMtr(bp, lock.SystemReservedTrxId, redoLog)
	if err := createConstraints(constraintsMtr, ct, bp, fileId, input.Constraints); err != nil {
		constraintsMtr.UnpinAll()
		return nil, err
	}
	if err := constraintsMtr.Commit(); err != nil {
		return nil, err
	}

	return &Table{
		primaryIndex:     pi,
		secondaryIndexes: sis,
		catalog:          ct,
		undoLog:          undoLog,
		lock:             lockMgr,
		bufferPool:       bp,
		redoLog:          redoLog,
	}, nil
}

// createTableFile はテーブルのファイルを作成する
func createTableFile(ct *dictionary.Catalog, bp *buffer.Pool, redoLog *redo.Buffer, tableName string) (page.FileId, error) {
	path := filepath.Join(config.BaseDir, fmt.Sprintf("%s.db", tableName))
	mtr := buffer.NewWriteMtr(bp, lock.SystemReservedTrxId, redoLog)
	fileId, err := ct.AllocateFileId(mtr)
	if err != nil {
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
func registerTableMeta(
	mtr *buffer.Mtr,
	ct *dictionary.Catalog,
	fileId page.FileId,
	pi *primaryIndex,
	input CreateTableInput,
) error {
	// テーブルメタ
	if err := ct.TableMeta().Insert(mtr, dictionary.NewTableMetaRecord(input.TableName, pi.tree.MetaPageId(), len(input.ColNames))); err != nil {
		return err
	}

	// インデックスメタ
	indexId, err := ct.AllocateIndexId(mtr)
	if err != nil {
		return err
	}
	err = ct.IndexMeta().Insert(mtr, dictionary.NewIndexMetaRecord(
		fileId,
		indexId,
		dictionary.PrimaryIndexName,
		dictionary.IndexTypePrimary,
		input.PkCount,
		pi.tree.MetaPageId(),
	))
	if err != nil {
		return err
	}

	// カラムメタ
	for i, col := range input.ColNames {
		if err := ct.ColumnMeta().Insert(mtr, dictionary.NewColumnMetaRecord(fileId, col, i)); err != nil {
			return err
		}
	}
	return nil
}

// createSecondaryIndexes はセカンダリインデックスを作成する
//   - mtr: カタログ登録の書き込みを記録する Mtr。Commit は呼び出し側
func createSecondaryIndexes(
	mtr *buffer.Mtr,
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
		indexId, err := ct.AllocateIndexId(mtr)
		if err != nil {
			return nil, err
		}
		index, err := createSecondaryIndex(ct, bp, createSecondaryIndexInput{
			FileId:      fileId,
			PrimaryTree: pt,
			IndexId:     indexId,
			IndexName:   input.IndexName,
			Unique:      input.IndexType == dictionary.IndexTypeUnique,
			Lock:        lockMgr,
			UndoLog:     undoLog,
			RedoLog:     redoLog,
		})
		if err != nil {
			return nil, err
		}
		err = ct.IndexMeta().Insert(mtr, dictionary.NewIndexMetaRecord(
			fileId,
			indexId,
			input.IndexName,
			input.IndexType,
			len(input.ColNames),
			index.tree.MetaPageId(),
		))
		if err != nil {
			return nil, err
		}

		for i, keyCol := range input.ColNames {
			if err := ct.IndexKeyColumnMeta().Insert(mtr, dictionary.NewIndexKeyColumnMetaRecord(indexId, keyCol, i)); err != nil {
				return nil, err
			}
		}

		indexes = append(indexes, index)
	}
	return indexes, nil
}

// createConstraints は制約をカタログに登録する
//   - mtr: 登録の書き込みを記録する Mtr。Commit は呼び出し側
func createConstraints(mtr *buffer.Mtr, ct *dictionary.Catalog, bp *buffer.Pool, fileId page.FileId, inputs []CreateConstraintInput) error {
	for _, input := range inputs {
		refTable, err := fetchTable(ct, bp, input.ReferenceTableName)
		if err != nil {
			return err
		}
		err = ct.ConstraintMeta().Insert(mtr, dictionary.NewConstraintMetaRecord(
			fileId,
			input.ColumnName,
			input.ConstraintName,
			refTable.MetaPageId().FileId(),
			input.ReferenceColumnName,
		))
		if err != nil {
			return err
		}
	}
	return nil
}
