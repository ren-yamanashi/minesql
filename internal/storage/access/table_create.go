package access

import (
	"fmt"
	"path/filepath"

	"github.com/ren-yamanashi/minesql/internal/storage/btree"
	"github.com/ren-yamanashi/minesql/internal/storage/buffer"
	"github.com/ren-yamanashi/minesql/internal/storage/catalog"
	"github.com/ren-yamanashi/minesql/internal/storage/config"
	"github.com/ren-yamanashi/minesql/internal/storage/file"
	"github.com/ren-yamanashi/minesql/internal/storage/lock"
	"github.com/ren-yamanashi/minesql/internal/storage/page"
	"github.com/ren-yamanashi/minesql/internal/storage/undo"
)

type CreateConstraintInput struct {
	ColName        string // 制約のあるカラム名
	ConstraintName string // 制約名
	RefTableName   string // 制約により参照されるテーブル名
	RefColName     string // 制約により参照されるカラム名 (構成順通り)
}

type CreateIndexInput struct {
	IndexName string            // インデックス名
	ColNames  []string          // インデックスを構成するカラム名 (構成順通り)
	IndexType catalog.IndexType // インデックス種類
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
	bp *buffer.BufferPool,
	undo *undo.Manager,
	lock *lock.Manager,
	input CreateTableInput,
) (*Table, error) {
	ct, err := catalog.NewCatalog(bp)
	if err != nil {
		return nil, err
	}

	// ファイル作成
	fileId, err := createTableFile(ct, bp, input.TableName)
	if err != nil {
		return nil, err
	}

	// プライマリインデックス作成
	pi, err := createPrimaryIndex(ct, bp, fileId, input.PkCount, lock)
	if err != nil {
		return nil, err
	}

	// テーブルメタ・カラムメタをカタログに登録
	if err := registerTableMeta(ct, fileId, pi, input); err != nil {
		return nil, err
	}

	// セカンダリインデックス作成
	sis, err := createSecondaryIndexes(ct, bp, fileId, pi.tree, lock, input.Indexes)
	if err != nil {
		return nil, err
	}

	// 制約作成
	if err := createConstraints(ct, fileId, input.Constraints); err != nil {
		return nil, err
	}

	return &Table{
		primaryIndex:     pi,
		secondaryIndexes: sis,
		catalog:          ct,
		undoLog:          undo,
		lock:             lock,
		bufferPool:       bp,
	}, nil
}

// createTableFile はテーブルのファイルを作成する
func createTableFile(ct *catalog.Catalog, bp *buffer.BufferPool, tableName string) (page.FileId, error) {
	path := filepath.Join(config.BaseDir, fmt.Sprintf("%s.db", tableName))
	fileId, err := ct.AllocateFileId()
	if err != nil {
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
func registerTableMeta(
	ct *catalog.Catalog,
	fileId page.FileId,
	pi *primaryIndex,
	input CreateTableInput,
) error {
	// テーブルメタ
	if err := ct.TableMeta.Insert(input.TableName, pi.tree.MetaPageId, len(input.ColNames)); err != nil {
		return err
	}

	// インデックスメタ
	indexId, err := ct.AllocateIndexId()
	if err != nil {
		return err
	}
	err = ct.IndexMeta.Insert(catalog.IndexRecord{
		FileId:     fileId,
		Name:       catalog.PrimaryIndexName,
		IndexId:    indexId,
		IndexType:  catalog.IndexTypePrimary,
		NumOfCol:   input.PkCount,
		MetaPageId: pi.tree.MetaPageId,
	})
	if err != nil {
		return err
	}

	// カラムメタ
	for i, col := range input.ColNames {
		if err := ct.ColumnMeta.Insert(fileId, col, i); err != nil {
			return err
		}
	}
	return nil
}

// createSecondaryIndexes はセカンダリインデックスを作成する
func createSecondaryIndexes(
	ct *catalog.Catalog,
	bp *buffer.BufferPool,
	fileId page.FileId,
	pt *btree.Btree,
	lock *lock.Manager,
	inputs []CreateIndexInput,
) ([]*secondaryIndex, error) {
	indexes := make([]*secondaryIndex, 0, len(inputs))
	for _, input := range inputs {
		indexId, err := ct.AllocateIndexId()
		if err != nil {
			return nil, err
		}
		index, err := createSecondaryIndex(ct, bp, createSecondaryIndexInput{
			FileId:      fileId,
			PrimaryTree: pt,
			IndexId:     indexId,
			IndexName:   input.IndexName,
			Unique:      input.IndexType == catalog.IndexTypeUnique,
			Lock:        lock,
		})
		if err != nil {
			return nil, err
		}
		err = ct.IndexMeta.Insert(catalog.IndexRecord{
			FileId:     fileId,
			Name:       input.IndexName,
			IndexId:    indexId,
			IndexType:  input.IndexType,
			NumOfCol:   len(input.ColNames),
			MetaPageId: index.tree.MetaPageId,
		})
		if err != nil {
			return nil, err
		}

		for i, keyCol := range input.ColNames {
			if err := ct.IndexKeyColMeta.Insert(indexId, keyCol, i); err != nil {
				return nil, err
			}
		}

		indexes = append(indexes, index)
	}

	return indexes, nil
}

// createConstraints は制約をカタログに登録する
func createConstraints(ct *catalog.Catalog, fileId page.FileId, inputs []CreateConstraintInput) error {
	for _, input := range inputs {
		refTable, err := fetchTable(ct, input.RefTableName)
		if err != nil {
			return err
		}
		err = ct.ConstraintMeta.Insert(catalog.ConstraintRecord{
			FileId:         fileId,
			ColName:        input.ColName,
			ConstraintName: input.ConstraintName,
			RefTableFileId: refTable.MetaPageId.FileId,
			RefColName:     input.RefColName,
		})
		if err != nil {
			return err
		}
	}
	return nil
}
