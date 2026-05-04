package access

import (
	"fmt"
	"path/filepath"

	"github.com/ren-yamanashi/minesql/internal/storage/btree"
	"github.com/ren-yamanashi/minesql/internal/storage/buffer"
	"github.com/ren-yamanashi/minesql/internal/storage/catalog"
	"github.com/ren-yamanashi/minesql/internal/storage/config"
	"github.com/ren-yamanashi/minesql/internal/storage/file"
	"github.com/ren-yamanashi/minesql/internal/storage/page"
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

func Create(bp *buffer.BufferPool, input CreateTableInput) (*Table, error) {
	ct, err := catalog.NewCatalog(bp)
	if err != nil {
		return nil, err
	}

	table, err := fetchTable(ct, input.TableName)
	if err != nil {
		return nil, err
	}

	// ファイル作成
	fileId, err := createTableFile(ct, bp, input.TableName)
	if err != nil {
		return nil, err
	}

	// プライマリインデックス作成
	pi, err := createPrimaryIndex(ct, bp, fileId, input)
	if err != nil {
		return nil, err
	}

	// セカンダリインデックス作成
	sis, err := createSecondaryIndexes(ct, bp, fileId, pi.tree, input.Indexes)
	if err != nil {
		return nil, err
	}

	// 制約作成
	if err := createConstraints(ct, fileId, input.Constraints); err != nil {
		return nil, err
	}

	return &Table{
		table:            table,
		primaryIndex:     pi,
		secondaryIndexes: sis,
		catalog:          ct,
		bufferPool:       bp,
	}, nil
}

// createTableFile はテーブルのファイルを作成する
func createTableFile(ct *catalog.Catalog, bp *buffer.BufferPool, tableName string) (page.FileId, error) {
	path := filepath.Join(config.BaseDir, fmt.Sprintf("%s.db", tableName))
	fileId := ct.AllocateFileId()
	hp, err := file.NewHeapFile(fileId, path)
	if err != nil {
		return 0, err
	}
	bp.RegisterHeapFile(fileId, hp)
	return fileId, nil
}

// createPrimaryIndex はプライマリインデックスを作成する
func createPrimaryIndex(ct *catalog.Catalog, bp *buffer.BufferPool, fileId page.FileId, input CreateTableInput) (*PrimaryIndex, error) {
	index, err := CreatePrimaryIndex(ct, bp, fileId, input.PkCount)
	if err != nil {
		return nil, err
	}

	// カタログに挿入
	err = ct.TableMeta.Insert(input.TableName, index.tree.MetaPageId, len(input.ColNames))
	if err != nil {
		return nil, err
	}
	err = ct.IndexMeta.Insert(
		fileId,
		catalog.PrimaryIndexName,
		ct.AllocateIndexId(),
		catalog.IndexTypePrimary,
		input.PkCount,
		index.tree.MetaPageId,
	)
	if err != nil {
		return nil, err
	}
	for i, col := range input.ColNames {
		if err := ct.ColumnMeta.Insert(fileId, col, i); err != nil {
			return nil, err
		}
	}
	return index, nil
}

// createSecondaryIndexes はセカンダリインデックスを作成する
func createSecondaryIndexes(
	ct *catalog.Catalog,
	bp *buffer.BufferPool,
	fileId page.FileId,
	pt *btree.Btree,
	inputs []CreateIndexInput,
) ([]*SecondaryIndex, error) {
	indexes := make([]*SecondaryIndex, 0, len(inputs))
	for _, input := range inputs {
		index, err := CreateSecondaryIndex(ct, bp, CreateSecondaryIndexInput{
			FileId:      fileId,
			PrimaryTree: pt,
			IndexName:   input.IndexName,
			IsUnique:    input.IndexType == catalog.IndexTypeUnique,
		})
		if err != nil {
			return nil, err
		}

		indexId := ct.AllocateIndexId()
		err = ct.IndexMeta.Insert(
			fileId,
			input.IndexName,
			indexId,
			input.IndexType,
			len(input.ColNames),
			index.tree.MetaPageId,
		)
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
		err = ct.ConstraintMeta.Insert(
			fileId,
			input.ColName,
			input.ConstraintName,
			refTable.MetaPageId.FileId,
			input.RefColName,
		)
		if err != nil {
			return err
		}
	}
	return nil
}
