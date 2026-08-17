package dictionary

import (
	"encoding/binary"
	"fmt"

	"github.com/ren-yamanashi/minesql/internal/storage/buffer"
	"github.com/ren-yamanashi/minesql/internal/storage/fsp"
	"github.com/ren-yamanashi/minesql/internal/storage/lock"
	"github.com/ren-yamanashi/minesql/internal/storage/page"
)

const (
	headerTableMetaOffset             = 0
	headerIndexMetaOffset             = 4
	headerIndexKeyColumnMetaOffset    = 8
	headerColumnMetaOffset            = 12
	headerConstraintMetaOffset        = 16
	headerUserMetaOffset              = 20
	headerNextFileIdOffset            = 24
	headerNextIndexIdOffset           = 28
	headerUndoLogFileIdOffset         = 32
	headerDDLUndoRootPageNumberOffset = 36
	headerNextTrxIdOffset             = 40
	headerFieldSize                   = 4
)

// CatalogFileId はカタログヘッダーが置かれる FileId (= DB 全体で固定値)
var CatalogFileId = page.FileId(0)

var catalogHeaderPageNum = page.PageNumber(1)

type Catalog struct {
	nextFileId         page.FileId
	nextIndexId        IndexId
	nextTrxId          lock.TrxId
	undoLogFileId      page.FileId
	ddlUndoRootPageId  page.Id
	tableMeta          *TableMeta
	indexMeta          *IndexMeta
	indexKeyColumnMeta *IndexKeyColumnMeta
	columnMeta         *ColumnMeta
	constraintMeta     *ConstraintMeta
	userMeta           *UserMeta
}

func (c *Catalog) UndoLogFileId() page.FileId              { return c.undoLogFileId }
func (c *Catalog) NextTrxId() lock.TrxId                   { return c.nextTrxId }
func (c *Catalog) TableMeta() *TableMeta                   { return c.tableMeta }
func (c *Catalog) IndexMeta() *IndexMeta                   { return c.indexMeta }
func (c *Catalog) IndexKeyColumnMeta() *IndexKeyColumnMeta { return c.indexKeyColumnMeta }
func (c *Catalog) ColumnMeta() *ColumnMeta                 { return c.columnMeta }
func (c *Catalog) ConstraintMeta() *ConstraintMeta         { return c.constraintMeta }
func (c *Catalog) UserMeta() *UserMeta                     { return c.userMeta }

// NewCatalog は既存のカタログを開く
//   - bp: メタテーブルの B+Tree が紐づくバッファプール
func NewCatalog(bp *buffer.Pool) (*Catalog, error) {
	mtr := buffer.NewMtr(bp)
	defer mtr.UnpinAll()

	if err := fsp.ValidateHeader(mtr, CatalogFileId); err != nil {
		return nil, err
	}

	headerPageId := page.NewId(CatalogFileId, catalogHeaderPageNum)
	bufPageHeader, err := mtr.PageForRead(headerPageId)
	if err != nil {
		return nil, err
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
	ddlUndoRootPageNumber := readPageNumber(bufPageHeader.Data().Body(), headerDDLUndoRootPageNumberOffset)
	nextTrxId := lock.TrxId(binary.BigEndian.Uint32(
		bufPageHeader.Data().Body()[headerNextTrxIdOffset : headerNextTrxIdOffset+headerFieldSize],
	))

	ddlUndoRootPageId := ddlUndoRootPageIdFromPageNumber(ddlUndoRootPageNumber)

	return &Catalog{
		nextFileId:        nextFileId,
		nextIndexId:       nextIndexId,
		nextTrxId:         nextTrxId,
		undoLogFileId:     undoLogFileId,
		ddlUndoRootPageId: ddlUndoRootPageId,
		tableMeta:         NewTableMeta(bp, page.NewId(CatalogFileId, tableMetaPageNumber)),
		indexMeta:         NewIndexMeta(bp, page.NewId(CatalogFileId, indexMetaPageNumber)),
		indexKeyColumnMeta: NewIndexKeyColumnMeta(
			bp, page.NewId(CatalogFileId, indexKeyColumnMetaPageNumber),
		),
		columnMeta:     NewColumnMeta(bp, page.NewId(CatalogFileId, columnMetaPageNumber)),
		constraintMeta: NewConstraintMeta(bp, page.NewId(CatalogFileId, constraintMetaPageNumber)),
		userMeta:       NewUserMeta(bp, page.NewId(CatalogFileId, userMetaPageNumber)),
	}, nil
}

// CreateCatalog はカタログを新規作成する
//   - mtr: FSP ヘッダー初期化・ヘッダーページ確保・各メタテーブル作成を記録する Mtr。Commit / UnpinAll は呼び出し側
//   - mtr の trxId に応じた Redo が記録される (= ブートストラップでは SystemReservedTrxId)
//   - ヘッダーページは PageNumber == 1 で確保される
func CreateCatalog(mtr *buffer.Mtr) (*Catalog, error) {
	bp := mtr.Pool()

	if err := fsp.InitHeader(mtr, CatalogFileId); err != nil {
		return nil, err
	}
	headerPageId, err := fsp.AllocatePage(mtr, CatalogFileId)
	if err != nil {
		return nil, err
	}
	if headerPageId.PageNumber() != catalogHeaderPageNum {
		panic(fmt.Sprintf("dictionary: catalog header page must be PageNumber %d, got %d", catalogHeaderPageNum, headerPageId.PageNumber()))
	}
	if _, err := bp.AddPage(headerPageId); err != nil {
		return nil, err
	}
	bufPageHeader, err := mtr.PageForWrite(headerPageId)
	if err != nil {
		return nil, err
	}

	tableMeta, err := CreateTableMeta(mtr)
	if err != nil {
		return nil, err
	}
	indexMeta, err := CreateIndexMeta(mtr)
	if err != nil {
		return nil, err
	}
	indexKeyColumnMeta, err := CreateIndexKeyColumnMeta(mtr)
	if err != nil {
		return nil, err
	}
	columnMeta, err := CreateColumnMeta(mtr)
	if err != nil {
		return nil, err
	}
	constraintMeta, err := CreateConstraintMeta(mtr)
	if err != nil {
		return nil, err
	}
	userMeta, err := CreateUserMeta(mtr)
	if err != nil {
		return nil, err
	}

	ddlUndoRootPageId, err := allocateAndInitializeDDLUndoRootPage(mtr, bp)
	if err != nil {
		return nil, err
	}

	nextFileId := page.FileId(1) // FileId(0) はカタログ用なので 1 から開始
	nextIndexId := IndexId(1)    // IndexId(0) は無効値として予約
	nextTrxId := lock.TrxId(1)   // 0 は予約値のため 1 から開始
	undoLogFileId := nextFileId  // Undo ログ用の FileId を採番
	nextFileId++

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
	writePageNumber(bufPageHeader, headerDDLUndoRootPageNumberOffset, ddlUndoRootPageId.PageNumber())
	writeScalar(bufPageHeader, headerNextTrxIdOffset, uint32(nextTrxId))

	return &Catalog{
		nextFileId:         nextFileId,
		nextIndexId:        nextIndexId,
		nextTrxId:          nextTrxId,
		undoLogFileId:      undoLogFileId,
		ddlUndoRootPageId:  ddlUndoRootPageId,
		tableMeta:          tableMeta,
		indexMeta:          indexMeta,
		indexKeyColumnMeta: indexKeyColumnMeta,
		columnMeta:         columnMeta,
		constraintMeta:     constraintMeta,
		userMeta:           userMeta,
	}, nil
}

// PersistNextTrxId は次に払い出すトランザクション ID をヘッダーページに永続化する
//   - mtr: 書き込みを記録する Mtr。Commit は呼び出し側
func (c *Catalog) PersistNextTrxId(mtr *buffer.Mtr, value lock.TrxId) error {
	if err := c.persistScalar(mtr, headerNextTrxIdOffset, uint32(value)); err != nil {
		return err
	}
	c.nextTrxId = value
	return nil
}

// AllocateIndexId は IndexId を採番し、ヘッダーページに永続化する
//   - mtr: 採番結果の書き込みを記録する Mtr。Commit は呼び出し側
//   - persistScalar 失敗時も nextIndexId は in-memory のインクリメント済み (= 単調増加放置、 ID は再利用しない)
func (c *Catalog) AllocateIndexId(mtr *buffer.Mtr) (IndexId, error) {
	id := c.nextIndexId
	c.nextIndexId++
	if err := c.persistScalar(mtr, headerNextIndexIdOffset, uint32(c.nextIndexId)); err != nil {
		return 0, err
	}
	return id, nil
}

// AllocateFileId は FileId を採番し、ヘッダーページに永続化する
//   - mtr: 採番結果の書き込みを記録する Mtr。Commit は呼び出し側
//   - persistScalar 失敗時も nextFileId は in-memory のインクリメント済み (= 単調増加放置、 ID は再利用しない)
func (c *Catalog) AllocateFileId(mtr *buffer.Mtr) (page.FileId, error) {
	id := c.nextFileId
	c.nextFileId++
	if err := c.persistScalar(mtr, headerNextFileIdOffset, uint32(c.nextFileId)); err != nil {
		return 0, err
	}
	return id, nil
}

// persistScalar はヘッダーページの指定オフセットに uint32 値を書き込む
//   - mtr: 書き込みを記録する Mtr。Commit は呼び出し側
func (c *Catalog) persistScalar(mtr *buffer.Mtr, offset int, value uint32) error {
	headerPageId := page.NewId(CatalogFileId, catalogHeaderPageNum)
	bufPageHeader, err := mtr.PageForWrite(headerPageId)
	if err != nil {
		return err
	}
	writeScalar(bufPageHeader, offset, value)
	return nil
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
