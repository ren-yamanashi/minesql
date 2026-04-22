package handler

import (
	"minesql/internal/storage/access"
	"minesql/internal/storage/dictionary"
)

// CreateIndexParam はインデックス作成パラメータ
type CreateIndexParam struct {
	Name    string // インデックス名
	ColName string // インデックスを構成するカラム名
	ColIdx  uint16 // インデックスカラムの位置 (0 始まりの列番号)
	Unique  bool   // ユニーク制約の有無
}

// CreateColumnParam はカラム作成パラメータ
type CreateColumnParam struct {
	Name string
	Type ColumnType
}

// CreateConstraintParam は外部キー制約の作成パラメータ
type CreateConstraintParam struct {
	ConstraintName string // 制約名
	ColName        string // 外部キーカラム名
	RefTableName   string // 参照先テーブル名
	RefColName     string // 参照先カラム名
}

// CreateTable はテーブルを新規作成し、カタログに登録する
func (h *Handler) CreateTable(tableName string, pkCount uint8, idxParams []CreateIndexParam, colParams []CreateColumnParam, constraintParams []CreateConstraintParam) error {
	// FileId を採番
	fileId, err := h.Catalog.AllocateFileId(h.BufferPool)
	if err != nil {
		return err
	}

	// Disk を登録
	if err := h.RegisterDmToBp(fileId, tableName); err != nil {
		return err
	}

	// テーブルの metaPageId を設定
	metaPageId, err := h.BufferPool.AllocatePageId(fileId)
	if err != nil {
		return err
	}

	// 各セカンダリインデックスを作成
	secondaryIndexes := make([]*access.SecondaryIndex, len(idxParams))
	for i, param := range idxParams {
		indexMetaPageId, err := h.BufferPool.AllocatePageId(fileId)
		if err != nil {
			return err
		}
		si := access.NewSecondaryIndex(param.Name, param.ColName, indexMetaPageId, param.ColIdx, pkCount, param.Unique)
		if err := si.Create(h.BufferPool); err != nil {
			return err
		}
		secondaryIndexes[i] = si
	}

	// テーブルを作成
	tbl := access.NewTable(tableName, metaPageId, pkCount, secondaryIndexes, nil, nil)
	if err := tbl.Create(h.BufferPool); err != nil {
		return err
	}

	// インデックスメタデータを作成
	idxMeta := make([]*dictionary.IndexMeta, len(idxParams))
	for i, param := range idxParams {
		idxType := dictionary.IndexTypeNonUnique
		if param.Unique {
			idxType = dictionary.IndexTypeUnique
		}
		idxMeta[i] = dictionary.NewIndexMeta(fileId, secondaryIndexes[i].Name, secondaryIndexes[i].ColName, idxType, secondaryIndexes[i].MetaPageId)
	}

	// カラムメタデータを作成
	colMeta := make([]*dictionary.ColumnMeta, len(colParams))
	for i, col := range colParams {
		colMeta[i] = dictionary.NewColumnMeta(fileId, col.Name, uint16(i), col.Type)
	}

	// 制約メタデータを作成
	var conMeta []*dictionary.ConstraintMeta

	// PK 制約を自動生成
	for i := uint8(0); i < pkCount && int(i) < len(colParams); i++ {
		conMeta = append(conMeta, dictionary.NewConstraintMeta(fileId, colParams[i].Name, string(dictionary.ConstraintTypePrimaryKey), "", ""))
	}

	// UK 制約を自動生成
	for _, param := range idxParams {
		if param.Unique {
			conMeta = append(conMeta, dictionary.NewConstraintMeta(fileId, param.ColName, param.Name, "", ""))
		}
	}

	// FK 制約を追加
	for _, param := range constraintParams {
		conMeta = append(conMeta, dictionary.NewConstraintMeta(fileId, param.ColName, param.ConstraintName, param.RefTableName, param.RefColName))
	}

	// テーブルメタデータを作成してカタログに登録
	tblMeta := dictionary.NewTableMeta(fileId, tableName, uint8(len(colParams)), pkCount, colMeta, idxMeta, metaPageId)
	tblMeta.Constraints = conMeta
	return h.Catalog.Insert(h.BufferPool, tblMeta)
}
