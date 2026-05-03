package catalog

import (
	"encoding/binary"
	"errors"

	"github.com/ren-yamanashi/minesql/internal/storage/buffer"
	"github.com/ren-yamanashi/minesql/internal/storage/page"
)

var (
	catalogFileId         = page.FileId(page.FileId(0))
	catalogHeaderPageNum  = page.PageNumber(0)
	ErrInvalidCatalogFile = errors.New("invalid database catalog file: magic number mismatch")
)

type Catalog struct {
	nextFileId      page.FileId
	nextIndexId     IndexId
	undoLogFileId   page.FileId
	tableMeta       *TableMeta
	indexMeta       *IndexMeta
	indexKeyColMeta *IndexKeyColMeta
	columnMeta      *ColumnMeta
	constraintMeta  *ConstraintMeta
	userMeta        *UserMeta
}

// NewCatalog は既存のカタログを開く
func NewCatalog(bp *buffer.BufferPool) (*Catalog, error) {
	headerPageId := page.NewPageId(catalogFileId, catalogHeaderPageNum)
	pageHeader, err := bp.GetReadPage(headerPageId)
	if err != nil {
		return nil, err
	}
	defer bp.UnRefPage(headerPageId)
	if string(pageHeader.Body[0:4]) != "MINE" {
		return nil, ErrInvalidCatalogFile
	}

	tableMetaPageNumber := page.PageNumber(binary.BigEndian.Uint32(pageHeader.Body[4:8]))
	indexMetaPageNumber := page.PageNumber(binary.BigEndian.Uint32(pageHeader.Body[8:12]))
	indexKeyColMetaPageNumber := page.PageNumber(binary.BigEndian.Uint32(pageHeader.Body[12:16]))
	columnMetaPageNumber := page.PageNumber(binary.BigEndian.Uint32(pageHeader.Body[16:20]))
	constraintMetaPageNumber := page.PageNumber(binary.BigEndian.Uint32(pageHeader.Body[20:24]))
	userMetaPageNumber := page.PageNumber(binary.BigEndian.Uint32(pageHeader.Body[24:28]))
	nextFileId := page.FileId(binary.BigEndian.Uint32(pageHeader.Body[28:32]))
	nextIndexId := IndexId(binary.BigEndian.Uint32(pageHeader.Body[32:36]))
	undoLogFileId := page.FileId(binary.BigEndian.Uint32(pageHeader.Body[36:40]))

	return &Catalog{
		nextFileId:      nextFileId,
		nextIndexId:     nextIndexId,
		undoLogFileId:   undoLogFileId,
		tableMeta:       NewTableMeta(bp, page.NewPageId(catalogFileId, tableMetaPageNumber)),
		indexMeta:       NewIndexMeta(bp, page.NewPageId(catalogFileId, indexMetaPageNumber)),
		indexKeyColMeta: NewIndexKeyColMeta(bp, page.NewPageId(catalogFileId, indexKeyColMetaPageNumber)),
		columnMeta:      NewColumnMeta(bp, page.NewPageId(catalogFileId, columnMetaPageNumber)),
		constraintMeta:  NewConstraintMeta(bp, page.NewPageId(catalogFileId, constraintMetaPageNumber)),
		userMeta:        NewUserMeta(bp, page.NewPageId(catalogFileId, userMetaPageNumber)),
	}, nil
}

// CreateCatalog はカタログを新規作成する
func CreateCatalog(bp *buffer.BufferPool) (*Catalog, error) {
	headerPageId, err := bp.AllocatePageId(catalogFileId)
	if err != nil {
		return nil, err
	}
	_, err = bp.AddPage(headerPageId)
	if err != nil {
		return nil, err
	}
	defer bp.UnRefPage(headerPageId)

	pageHeader, err := bp.GetWritePage(headerPageId)
	if err != nil {
		return nil, err
	}

	tableMeta, err := CreateTableMeta(bp)
	if err != nil {
		return nil, err
	}
	indexMeta, err := CreateIndexMeta(bp)
	if err != nil {
		return nil, err
	}
	indexKeyColMeta, err := CreateIndexKeyColMeta(bp)
	if err != nil {
		return nil, err
	}
	columnMeta, err := CreateColumnMeta(bp)
	if err != nil {
		return nil, err
	}
	constraintMeta, err := CreateConstraintMeta(bp)
	if err != nil {
		return nil, err
	}
	userMeta, err := CreateUserMeta(bp)
	if err != nil {
		return nil, err
	}

	nextFileId := page.FileId(1) // FileId(0) はカタログ用なので 1 から開始
	nextIndexId := IndexId(0)
	undoLogFileId := nextFileId // Undo ログ用の FileId を採番
	nextFileId++
	copy(pageHeader.Body[0:4], []byte("MINE"))
	binary.BigEndian.PutUint32(pageHeader.Body[4:8], uint32(tableMeta.metaPageId.PageNumber))
	binary.BigEndian.PutUint32(pageHeader.Body[8:12], uint32(indexMeta.metaPageId.PageNumber))
	binary.BigEndian.PutUint32(pageHeader.Body[12:16], uint32(indexKeyColMeta.metaPageId.PageNumber))
	binary.BigEndian.PutUint32(pageHeader.Body[16:20], uint32(columnMeta.metaPageId.PageNumber))
	binary.BigEndian.PutUint32(pageHeader.Body[20:24], uint32(constraintMeta.metaPageId.PageNumber))
	binary.BigEndian.PutUint32(pageHeader.Body[24:28], uint32(userMeta.metaPageId.PageNumber))
	binary.BigEndian.PutUint32(pageHeader.Body[28:32], uint32(nextFileId))
	binary.BigEndian.PutUint32(pageHeader.Body[32:36], uint32(nextIndexId))
	binary.BigEndian.PutUint32(pageHeader.Body[36:40], uint32(undoLogFileId))

	return &Catalog{
		nextFileId:      nextFileId,
		nextIndexId:     nextIndexId,
		undoLogFileId:   undoLogFileId,
		tableMeta:       tableMeta,
		indexMeta:       indexMeta,
		indexKeyColMeta: indexKeyColMeta,
		columnMeta:      columnMeta,
		constraintMeta:  constraintMeta,
		userMeta:        userMeta,
	}, nil
}
