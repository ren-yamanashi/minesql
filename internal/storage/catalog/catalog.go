package catalog

import (
	"bytes"
	"encoding/binary"
	"errors"

	"github.com/ren-yamanashi/minesql/internal/storage/buffer"
	"github.com/ren-yamanashi/minesql/internal/storage/page"
)

var (
	catalogFileId         = page.FileId(0)
	catalogHeaderPageNum  = page.PageNumber(0)
	ErrInvalidCatalogFile = errors.New("invalid database catalog file: magic number mismatch")
	catalogMagicNumber    = []byte("MINE")
)

// ヘッダーページ内のオフセット
const (
	headerMagicNumberOffset     = 0
	headerTableMetaOffset       = 4
	headerIndexMetaOffset       = 8
	headerIndexKeyColMetaOffset = 12
	headerColumnMetaOffset      = 16
	headerConstraintMetaOffset  = 20
	headerUserMetaOffset        = 24
	headerNextFileIdOffset      = 28
	headerNextIndexIdOffset     = 32
	headerUndoLogFileIdOffset   = 36
	headerFieldSize             = 4 // 各フィールドのバイト数
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

	magicEnd := headerMagicNumberOffset + len(catalogMagicNumber)
	if !bytes.Equal(pageHeader.Body[headerMagicNumberOffset:magicEnd], catalogMagicNumber) {
		return nil, ErrInvalidCatalogFile
	}

	tableMetaPageNumber := readPageNumber(pageHeader.Body, headerTableMetaOffset)
	indexMetaPageNumber := readPageNumber(pageHeader.Body, headerIndexMetaOffset)
	indexKeyColMetaPageNumber := readPageNumber(pageHeader.Body, headerIndexKeyColMetaOffset)
	columnMetaPageNumber := readPageNumber(pageHeader.Body, headerColumnMetaOffset)
	constraintMetaPageNumber := readPageNumber(pageHeader.Body, headerConstraintMetaOffset)
	userMetaPageNumber := readPageNumber(pageHeader.Body, headerUserMetaOffset)
	nextFileId := page.FileId(binary.BigEndian.Uint32(pageHeader.Body[headerNextFileIdOffset : headerNextFileIdOffset+headerFieldSize]))
	nextIndexId := IndexId(binary.BigEndian.Uint32(pageHeader.Body[headerNextIndexIdOffset : headerNextIndexIdOffset+headerFieldSize]))
	undoLogFileId := page.FileId(binary.BigEndian.Uint32(pageHeader.Body[headerUndoLogFileIdOffset : headerUndoLogFileIdOffset+headerFieldSize]))

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

	copy(pageHeader.Body[headerMagicNumberOffset:], catalogMagicNumber)
	writePageNumber(pageHeader.Body, headerTableMetaOffset, tableMeta.metaPageId.PageNumber)
	writePageNumber(pageHeader.Body, headerIndexMetaOffset, indexMeta.metaPageId.PageNumber)
	writePageNumber(pageHeader.Body, headerIndexKeyColMetaOffset, indexKeyColMeta.metaPageId.PageNumber)
	writePageNumber(pageHeader.Body, headerColumnMetaOffset, columnMeta.metaPageId.PageNumber)
	writePageNumber(pageHeader.Body, headerConstraintMetaOffset, constraintMeta.metaPageId.PageNumber)
	writePageNumber(pageHeader.Body, headerUserMetaOffset, userMeta.metaPageId.PageNumber)
	binary.BigEndian.PutUint32(pageHeader.Body[headerNextFileIdOffset:headerNextFileIdOffset+headerFieldSize], uint32(nextFileId))
	binary.BigEndian.PutUint32(pageHeader.Body[headerNextIndexIdOffset:headerNextIndexIdOffset+headerFieldSize], uint32(nextIndexId))
	binary.BigEndian.PutUint32(pageHeader.Body[headerUndoLogFileIdOffset:headerUndoLogFileIdOffset+headerFieldSize], uint32(undoLogFileId))

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

func readPageNumber(body []byte, offset int) page.PageNumber {
	return page.PageNumber(binary.BigEndian.Uint32(body[offset : offset+headerFieldSize]))
}

func writePageNumber(body []byte, offset int, pn page.PageNumber) {
	binary.BigEndian.PutUint32(body[offset:offset+headerFieldSize], uint32(pn))
}
