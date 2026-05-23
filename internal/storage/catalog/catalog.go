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
	bufferPool      *buffer.Pool
	nextFileId      page.FileId
	nextIndexId     IndexId
	UndoLogFileId   page.FileId
	TableMeta       *TableMeta
	IndexMeta       *IndexMeta
	IndexKeyColMeta *IndexKeyColMeta
	ColumnMeta      *ColumnMeta
	ConstraintMeta  *ConstraintMeta
	UserMeta        *UserMeta
}

// NewCatalog は既存のカタログを開く
func NewCatalog(bp *buffer.Pool) (*Catalog, error) {
	headerPageId := page.NewId(catalogFileId, catalogHeaderPageNum)
	bufPageHeader, err := bp.PageForRead(headerPageId)
	if err != nil {
		return nil, err
	}
	defer bp.UnRefPage(headerPageId)

	magicEnd := headerMagicNumberOffset + len(catalogMagicNumber)
	if !bytes.Equal(bufPageHeader.Page.Body[headerMagicNumberOffset:magicEnd], catalogMagicNumber) {
		return nil, ErrInvalidCatalogFile
	}

	tableMetaPageNumber := readPageNumber(bufPageHeader.Page.Body, headerTableMetaOffset)
	indexMetaPageNumber := readPageNumber(bufPageHeader.Page.Body, headerIndexMetaOffset)
	indexKeyColMetaPageNumber := readPageNumber(bufPageHeader.Page.Body, headerIndexKeyColMetaOffset)
	columnMetaPageNumber := readPageNumber(bufPageHeader.Page.Body, headerColumnMetaOffset)
	constraintMetaPageNumber := readPageNumber(bufPageHeader.Page.Body, headerConstraintMetaOffset)
	userMetaPageNumber := readPageNumber(bufPageHeader.Page.Body, headerUserMetaOffset)
	nextFileId := page.FileId(binary.BigEndian.Uint32(
		bufPageHeader.Page.Body[headerNextFileIdOffset : headerNextFileIdOffset+headerFieldSize],
	))
	nextIndexId := IndexId(binary.BigEndian.Uint32(
		bufPageHeader.Page.Body[headerNextIndexIdOffset : headerNextIndexIdOffset+headerFieldSize],
	))
	undoLogFileId := page.FileId(binary.BigEndian.Uint32(
		bufPageHeader.Page.Body[headerUndoLogFileIdOffset : headerUndoLogFileIdOffset+headerFieldSize],
	))

	return &Catalog{
		bufferPool:      bp,
		nextFileId:      nextFileId,
		nextIndexId:     nextIndexId,
		UndoLogFileId:   undoLogFileId,
		TableMeta:       NewTableMeta(bp, page.NewId(catalogFileId, tableMetaPageNumber)),
		IndexMeta:       NewIndexMeta(bp, page.NewId(catalogFileId, indexMetaPageNumber)),
		IndexKeyColMeta: NewIndexKeyColMeta(bp, page.NewId(catalogFileId, indexKeyColMetaPageNumber)),
		ColumnMeta:      NewColumnMeta(bp, page.NewId(catalogFileId, columnMetaPageNumber)),
		ConstraintMeta:  NewConstraintMeta(bp, page.NewId(catalogFileId, constraintMetaPageNumber)),
		UserMeta:        NewUserMeta(bp, page.NewId(catalogFileId, userMetaPageNumber)),
	}, nil
}

// CreateCatalog はカタログを新規作成する
func CreateCatalog(bp *buffer.Pool) (*Catalog, error) {
	headerPageId, err := bp.AllocatePageId(catalogFileId)
	if err != nil {
		return nil, err
	}
	_, err = bp.AddPage(headerPageId)
	if err != nil {
		return nil, err
	}
	defer bp.UnRefPage(headerPageId)

	bufPageHeader, err := bp.PageForWrite(headerPageId)
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

	copy(bufPageHeader.Page.Body[headerMagicNumberOffset:], catalogMagicNumber)
	writePageNumber(bufPageHeader.Page.Body, headerTableMetaOffset, tableMeta.tree.MetaPageId().PageNumber)
	writePageNumber(bufPageHeader.Page.Body, headerIndexMetaOffset, indexMeta.tree.MetaPageId().PageNumber)
	writePageNumber(bufPageHeader.Page.Body, headerIndexKeyColMetaOffset, indexKeyColMeta.tree.MetaPageId().PageNumber)
	writePageNumber(bufPageHeader.Page.Body, headerColumnMetaOffset, columnMeta.tree.MetaPageId().PageNumber)
	writePageNumber(bufPageHeader.Page.Body, headerConstraintMetaOffset, constraintMeta.tree.MetaPageId().PageNumber)
	writePageNumber(bufPageHeader.Page.Body, headerUserMetaOffset, userMeta.tree.MetaPageId().PageNumber)
	writeScalar(bufPageHeader.Page.Body, headerNextFileIdOffset, uint32(nextFileId))
	writeScalar(bufPageHeader.Page.Body, headerNextIndexIdOffset, uint32(nextIndexId))
	writeScalar(bufPageHeader.Page.Body, headerUndoLogFileIdOffset, uint32(undoLogFileId))

	return &Catalog{
		bufferPool:      bp,
		nextFileId:      nextFileId,
		nextIndexId:     nextIndexId,
		UndoLogFileId:   undoLogFileId,
		TableMeta:       tableMeta,
		IndexMeta:       indexMeta,
		IndexKeyColMeta: indexKeyColMeta,
		ColumnMeta:      columnMeta,
		ConstraintMeta:  constraintMeta,
		UserMeta:        userMeta,
	}, nil
}

// AllocateIndexId は IndexId を採番し、ヘッダーページに永続化する
func (c *Catalog) AllocateIndexId() (IndexId, error) {
	id := c.nextIndexId
	c.nextIndexId++
	if err := c.persistScalar(headerNextIndexIdOffset, uint32(c.nextIndexId)); err != nil {
		return 0, err
	}
	return id, nil
}

// AllocateFileId は FileId を採番し、ヘッダーページに永続化する
func (c *Catalog) AllocateFileId() (page.FileId, error) {
	id := c.nextFileId
	c.nextFileId++
	if err := c.persistScalar(headerNextFileIdOffset, uint32(c.nextFileId)); err != nil {
		return 0, err
	}
	return id, nil
}

// persistScalar はヘッダーページの指定オフセットに uint32 値を書き込む
func (c *Catalog) persistScalar(offset int, value uint32) error {
	headerPageId := page.NewId(catalogFileId, catalogHeaderPageNum)
	bufPageHeader, err := c.bufferPool.PageForWrite(headerPageId)
	if err != nil {
		return err
	}
	defer c.bufferPool.UnRefPage(headerPageId)
	writeScalar(bufPageHeader.Page.Body, offset, value)
	return nil
}

// writeScalar はヘッダーページの指定オフセットに uint32 値を書き込む
func writeScalar(body []byte, offset int, value uint32) {
	binary.BigEndian.PutUint32(body[offset:offset+headerFieldSize], value)
}

// readPageNumber はヘッダーページの指定オフセットから PageNumber を読み取る
func readPageNumber(body []byte, offset int) page.PageNumber {
	return page.PageNumber(binary.BigEndian.Uint32(body[offset : offset+headerFieldSize]))
}

// writePageNumber はヘッダーページの指定オフセットに PageNumber を書き込む
func writePageNumber(body []byte, offset int, pn page.PageNumber) {
	binary.BigEndian.PutUint32(body[offset:offset+headerFieldSize], uint32(pn))
}
