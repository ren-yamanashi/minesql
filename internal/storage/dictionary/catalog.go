package dictionary

import (
	"bytes"
	"encoding/binary"
	"errors"

	"github.com/ren-yamanashi/minesql/internal/storage/buffer"
	"github.com/ren-yamanashi/minesql/internal/storage/lock"
	"github.com/ren-yamanashi/minesql/internal/storage/page"
	"github.com/ren-yamanashi/minesql/internal/storage/redo"
)

const (
	headerMagicNumberOffset        = 0
	headerTableMetaOffset          = 4
	headerIndexMetaOffset          = 8
	headerIndexKeyColumnMetaOffset = 12
	headerColumnMetaOffset         = 16
	headerConstraintMetaOffset     = 20
	headerUserMetaOffset           = 24
	headerNextFileIdOffset         = 28
	headerNextIndexIdOffset        = 32
	headerUndoLogFileIdOffset      = 36
	headerFieldSize                = 4
)

var (
	catalogFileId         = page.FileId(0)
	catalogHeaderPageNum  = page.PageNumber(0)
	errInvalidCatalogFile = errors.New("invalid database catalog file: magic number mismatch")
	catalogMagicNumber    = []byte("MINE")
)

type Catalog struct {
	bufferPool         *buffer.Pool
	redoLog            *redo.Buffer // DDL (persistScalar / table_create 等) を Redo に記録するための参照
	nextFileId         page.FileId
	nextIndexId        IndexId
	undoLogFileId      page.FileId
	tableMeta          *TableMeta
	indexMeta          *IndexMeta
	indexKeyColumnMeta *IndexKeyColumnMeta
	columnMeta         *ColumnMeta
	constraintMeta     *ConstraintMeta
	userMeta           *UserMeta
}

func (c *Catalog) UndoLogFileId() page.FileId              { return c.undoLogFileId }
func (c *Catalog) TableMeta() *TableMeta                   { return c.tableMeta }
func (c *Catalog) IndexMeta() *IndexMeta                   { return c.indexMeta }
func (c *Catalog) IndexKeyColumnMeta() *IndexKeyColumnMeta { return c.indexKeyColumnMeta }
func (c *Catalog) ColumnMeta() *ColumnMeta                 { return c.columnMeta }
func (c *Catalog) ConstraintMeta() *ConstraintMeta         { return c.constraintMeta }
func (c *Catalog) UserMeta() *UserMeta                     { return c.userMeta }

// NewCatalog は既存のカタログを開く
//   - redoLog: 後の DDL (persistScalar / table_create 経由) を Redo に記録するために保持する
func NewCatalog(bp *buffer.Pool, redoLog *redo.Buffer) (*Catalog, error) {
	mtr := buffer.NewMtr(bp)
	defer mtr.UnpinAll()

	headerPageId := page.NewId(catalogFileId, catalogHeaderPageNum)
	bufPageHeader, err := mtr.PageForRead(headerPageId)
	if err != nil {
		return nil, err
	}

	magicEnd := headerMagicNumberOffset + len(catalogMagicNumber)
	if !bytes.Equal(bufPageHeader.Data().Body()[headerMagicNumberOffset:magicEnd], catalogMagicNumber) {
		return nil, errInvalidCatalogFile
	}

	tableMetaPageNumber := readPageNumber(bufPageHeader.Data().Body(), headerTableMetaOffset)
	indexMetaPageNumber := readPageNumber(bufPageHeader.Data().Body(), headerIndexMetaOffset)
	indexKeyColumnMetaPageNumber := readPageNumber(
		bufPageHeader.Data().Body(), headerIndexKeyColumnMetaOffset,
	)
	columnMetaPageNumber := readPageNumber(bufPageHeader.Data().Body(), headerColumnMetaOffset)
	constraintMetaPageNumber := readPageNumber(bufPageHeader.Data().Body(), headerConstraintMetaOffset)
	userMetaPageNumber := readPageNumber(bufPageHeader.Data().Body(), headerUserMetaOffset)
	nextFileId := page.FileId(binary.BigEndian.Uint32(
		bufPageHeader.Data().Body()[headerNextFileIdOffset : headerNextFileIdOffset+headerFieldSize],
	))
	nextIndexId := IndexId(binary.BigEndian.Uint32(
		bufPageHeader.Data().Body()[headerNextIndexIdOffset : headerNextIndexIdOffset+headerFieldSize],
	))
	undoLogFileId := page.FileId(binary.BigEndian.Uint32(
		bufPageHeader.Data().Body()[headerUndoLogFileIdOffset : headerUndoLogFileIdOffset+headerFieldSize],
	))

	return &Catalog{
		bufferPool:    bp,
		redoLog:       redoLog,
		nextFileId:    nextFileId,
		nextIndexId:   nextIndexId,
		undoLogFileId: undoLogFileId,
		tableMeta:     NewTableMeta(bp, page.NewId(catalogFileId, tableMetaPageNumber)),
		indexMeta:     NewIndexMeta(bp, page.NewId(catalogFileId, indexMetaPageNumber)),
		indexKeyColumnMeta: NewIndexKeyColumnMeta(
			bp, page.NewId(catalogFileId, indexKeyColumnMetaPageNumber),
		),
		columnMeta:     NewColumnMeta(bp, page.NewId(catalogFileId, columnMetaPageNumber)),
		constraintMeta: NewConstraintMeta(bp, page.NewId(catalogFileId, constraintMetaPageNumber)),
		userMeta:       NewUserMeta(bp, page.NewId(catalogFileId, userMetaPageNumber)),
	}, nil
}

// CreateCatalog はカタログを新規作成する
//   - redoLog: ヘッダー初期化と各メタテーブル作成を Redo に記録するために使う
//   - 内部のページ書き込みはシステム予約 trxId で記録する
func CreateCatalog(bp *buffer.Pool, redoLog *redo.Buffer) (*Catalog, error) {
	mtr := buffer.NewWriteMtr(bp, lock.SystemReservedTrxId, redoLog)

	headerPageId, err := bp.AllocatePageId(catalogFileId)
	if err != nil {
		mtr.UnpinAll()
		return nil, err
	}
	if _, err := bp.AddPage(headerPageId); err != nil {
		mtr.UnpinAll()
		return nil, err
	}
	bufPageHeader, err := mtr.PageForWrite(headerPageId)
	if err != nil {
		mtr.UnpinAll()
		return nil, err
	}

	tableMeta, err := CreateTableMeta(bp, redoLog)
	if err != nil {
		mtr.UnpinAll()
		return nil, err
	}
	indexMeta, err := CreateIndexMeta(bp, redoLog)
	if err != nil {
		mtr.UnpinAll()
		return nil, err
	}
	indexKeyColumnMeta, err := CreateIndexKeyColumnMeta(bp, redoLog)
	if err != nil {
		mtr.UnpinAll()
		return nil, err
	}
	columnMeta, err := CreateColumnMeta(bp, redoLog)
	if err != nil {
		mtr.UnpinAll()
		return nil, err
	}
	constraintMeta, err := CreateConstraintMeta(bp, redoLog)
	if err != nil {
		mtr.UnpinAll()
		return nil, err
	}
	userMeta, err := CreateUserMeta(bp, redoLog)
	if err != nil {
		mtr.UnpinAll()
		return nil, err
	}

	nextFileId := page.FileId(1) // FileId(0) はカタログ用なので 1 から開始
	nextIndexId := IndexId(1)    // IndexId(0) は無効値として予約
	undoLogFileId := nextFileId  // Undo ログ用の FileId を採番
	nextFileId++

	bufPageHeader.WriteBodyAt(headerMagicNumberOffset, catalogMagicNumber)
	writePageNumber(bufPageHeader, headerTableMetaOffset, tableMeta.tree.MetaPageId().PageNumber())
	writePageNumber(bufPageHeader, headerIndexMetaOffset, indexMeta.tree.MetaPageId().PageNumber())
	writePageNumber(
		bufPageHeader, headerIndexKeyColumnMetaOffset,
		indexKeyColumnMeta.tree.MetaPageId().PageNumber(),
	)
	writePageNumber(bufPageHeader, headerColumnMetaOffset, columnMeta.tree.MetaPageId().PageNumber())
	writePageNumber(
		bufPageHeader, headerConstraintMetaOffset, constraintMeta.tree.MetaPageId().PageNumber(),
	)
	writePageNumber(bufPageHeader, headerUserMetaOffset, userMeta.tree.MetaPageId().PageNumber())
	writeScalar(bufPageHeader, headerNextFileIdOffset, uint32(nextFileId))
	writeScalar(bufPageHeader, headerNextIndexIdOffset, uint32(nextIndexId))
	writeScalar(bufPageHeader, headerUndoLogFileIdOffset, uint32(undoLogFileId))

	if err := mtr.Commit(); err != nil {
		return nil, err
	}

	return &Catalog{
		bufferPool:         bp,
		redoLog:            redoLog,
		nextFileId:         nextFileId,
		nextIndexId:        nextIndexId,
		undoLogFileId:      undoLogFileId,
		tableMeta:          tableMeta,
		indexMeta:          indexMeta,
		indexKeyColumnMeta: indexKeyColumnMeta,
		columnMeta:         columnMeta,
		constraintMeta:     constraintMeta,
		userMeta:           userMeta,
	}, nil
}

// AllocateIndexId は IndexId を採番し、ヘッダーページに永続化する
func (c *Catalog) AllocateIndexId() (IndexId, error) {
	id := c.nextIndexId
	c.nextIndexId++
	if err := c.persistScalar(headerNextIndexIdOffset, uint32(c.nextIndexId)); err != nil {
		c.nextIndexId-- // rollback
		return 0, err
	}
	return id, nil
}

// AllocateFileId は FileId を採番し、ヘッダーページに永続化する
func (c *Catalog) AllocateFileId() (page.FileId, error) {
	id := c.nextFileId
	c.nextFileId++
	if err := c.persistScalar(headerNextFileIdOffset, uint32(c.nextFileId)); err != nil {
		c.nextFileId-- // rollback
		return 0, err
	}
	return id, nil
}

// persistScalar はヘッダーページの指定オフセットに uint32 値を書き込み Redo へ記録する
//   - システム予約 trxId で記録される
func (c *Catalog) persistScalar(offset int, value uint32) error {
	mtr := buffer.NewWriteMtr(c.bufferPool, lock.SystemReservedTrxId, c.redoLog)

	headerPageId := page.NewId(catalogFileId, catalogHeaderPageNum)
	bufPageHeader, err := mtr.PageForWrite(headerPageId)
	if err != nil {
		mtr.UnpinAll()
		return err
	}
	writeScalar(bufPageHeader, offset, value)
	return mtr.Commit()
}

// writeScalar はヘッダーページの指定オフセットに uint32 値を書き込む
func writeScalar(bp *buffer.Page, offset int, value uint32) {
	buf := make([]byte, headerFieldSize)
	binary.BigEndian.PutUint32(buf, value)
	bp.WriteBodyAt(offset, buf)
}

// readPageNumber はヘッダーページの指定オフセットから PageNumber を読み取る
func readPageNumber(body []byte, offset int) page.PageNumber {
	return page.PageNumber(binary.BigEndian.Uint32(body[offset : offset+headerFieldSize]))
}

// writePageNumber はヘッダーページの指定オフセットに PageNumber を書き込む
func writePageNumber(bp *buffer.Page, offset int, pn page.PageNumber) {
	writeScalar(bp, offset, uint32(pn))
}
