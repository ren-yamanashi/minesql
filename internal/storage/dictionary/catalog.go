package dictionary

import (
	"encoding/binary"
	"fmt"
	"minesql/internal/storage/btree"
	"minesql/internal/storage/buffer"
	"minesql/internal/storage/page"
)

var (
	ErrInvalidCatalogFile = fmt.Errorf("invalid database catalog file: magic number mismatch")
)

// Catalog はテーブルのメタデータ (テーブル情報、インデックス情報、カラム情報) を管理する
type Catalog struct {
	TableMetaPageId  page.PageId
	IndexMetaPageId  page.PageId
	ColumnMetaPageId page.PageId
	NextFileId       page.FileId
	UndoFileId       page.FileId
	metadata         []*TableMeta
}

// NewCatalog は既存のカタログを開く
func NewCatalog(bp *buffer.BufferPool) (*Catalog, error) {
	fileId := page.FileId(0) // カタログ専用の FileId を使用
	headerPageId := page.NewPageId(fileId, page.PageNumber(0))

	// ヘッダーページを読み込む
	bufPage, err := bp.FetchPage(headerPageId)
	if err != nil {
		return nil, err
	}
	defer bp.UnRefPage(headerPageId)

	// データを読み取る
	data := bufPage.GetReadData()
	if string(data[0:4]) != "MINE" {
		return nil, ErrInvalidCatalogFile
	}

	// 各メタデータの MetaPageId を取得
	tblMetaPageNum := binary.BigEndian.Uint32(data[4:8])
	idxMetaPageNum := binary.BigEndian.Uint32(data[8:12])
	colMetaPageNum := binary.BigEndian.Uint32(data[12:16])
	nextFileId := page.FileId(binary.BigEndian.Uint32(data[16:20]))
	undoFileId := page.FileId(binary.BigEndian.Uint32(data[20:24]))

	catalog := &Catalog{
		TableMetaPageId:  page.NewPageId(fileId, page.PageNumber(tblMetaPageNum)),
		IndexMetaPageId:  page.NewPageId(fileId, page.PageNumber(idxMetaPageNum)),
		ColumnMetaPageId: page.NewPageId(fileId, page.PageNumber(colMetaPageNum)),
		NextFileId:       nextFileId,
		UndoFileId:       undoFileId,
		metadata:         nil,
	}

	// ディスクから既存のメタデータを読み込む
	tableMeta, err := loadTableMeta(bp, catalog.TableMetaPageId, catalog.IndexMetaPageId, catalog.ColumnMetaPageId)
	if err != nil {
		return nil, err
	}
	catalog.metadata = tableMeta

	return catalog, nil
}

// CreateCatalog はカタログを新規作成する
func CreateCatalog(bp *buffer.BufferPool) (*Catalog, error) {
	fileId := page.FileId(0) // カタログ専用の FileId を使用

	// ヘッダーページを作成
	headerPageId, err := bp.AllocatePageId(fileId)
	if err != nil {
		return nil, err
	}
	bufferPage, err := bp.AddPage(headerPageId)
	if err != nil {
		return nil, err
	}
	defer bp.UnRefPage(headerPageId)

	// テーブルメタデータ用の B+Tree を作成
	tblMetaPageId, err := bp.AllocatePageId(fileId)
	if err != nil {
		return nil, err
	}
	tblMetaTree, err := btree.CreateBTree(bp, tblMetaPageId)
	if err != nil {
		return nil, err
	}

	// インデックスメタデータ用の B+Tree を作成
	idxMetaPageId, err := bp.AllocatePageId(fileId)
	if err != nil {
		return nil, err
	}
	idxMetaTree, err := btree.CreateBTree(bp, idxMetaPageId)
	if err != nil {
		return nil, err
	}

	// カラムメタデータ用の B+Tree を作成
	colMetaPageId, err := bp.AllocatePageId(fileId)
	if err != nil {
		return nil, err
	}
	colMetaTree, err := btree.CreateBTree(bp, colMetaPageId)
	if err != nil {
		return nil, err
	}

	// ヘッダーページに各メタデータのメタページIDを保存
	data := bufferPage.GetWriteData()
	nextFileId := page.FileId(1) // FileId(0) はカタログ用なので 1 から開始
	undoFileId := nextFileId     // UNDO ログ用の FileId を採番
	nextFileId++
	copy(data[0:4], []byte("MINE")) // ファイルシグネチャとしてマジックナンバーを設定 (minesql なので MINE)
	binary.BigEndian.PutUint32(data[4:8], uint32(tblMetaTree.MetaPageId.PageNumber))
	binary.BigEndian.PutUint32(data[8:12], uint32(idxMetaTree.MetaPageId.PageNumber))
	binary.BigEndian.PutUint32(data[12:16], uint32(colMetaTree.MetaPageId.PageNumber))
	binary.BigEndian.PutUint32(data[16:20], uint32(nextFileId))
	binary.BigEndian.PutUint32(data[20:24], uint32(undoFileId))

	return &Catalog{
		TableMetaPageId:  tblMetaTree.MetaPageId,
		ColumnMetaPageId: colMetaTree.MetaPageId,
		IndexMetaPageId:  idxMetaTree.MetaPageId,
		NextFileId:       nextFileId,
		UndoFileId:       undoFileId,
		metadata:         nil,
	}, nil
}

// Insert はカタログにメタデータを挿入する
func (c *Catalog) Insert(bp *buffer.BufferPool, tableMeta TableMeta) error {
	// 各メタデータに MetaPageId を設定する
	tableMeta.MetaPageId = c.TableMetaPageId
	for _, indexMeta := range tableMeta.Indexes {
		indexMeta.MetaPageId = c.IndexMetaPageId
	}
	for _, colMeta := range tableMeta.Cols {
		colMeta.MetaPageId = c.ColumnMetaPageId
	}

	// テーブルメタデータを挿入
	if err := tableMeta.Insert(bp); err != nil {
		return err
	}

	// インデックスメタデータを挿入
	for _, indexMeta := range tableMeta.Indexes {
		if err := indexMeta.Insert(bp); err != nil {
			return err
		}
	}

	// カラムメタデータを挿入
	for _, colMeta := range tableMeta.Cols {
		if err := colMeta.Insert(bp); err != nil {
			return err
		}
	}

	c.metadata = append(c.metadata, &tableMeta)
	return nil
}

// GetTableMetaByName はテーブル名からテーブルメタデータを取得する
func (c *Catalog) GetTableMetaByName(tableName string) (*TableMeta, bool) {
	for _, tblMeta := range c.metadata {
		if tblMeta.Name == tableName {
			return tblMeta, true
		}
	}
	return nil, false
}

// GetAllTables はすべてのテーブルメタデータを取得する
func (c *Catalog) GetAllTables() []*TableMeta {
	return c.metadata
}

// AllocateFileId は新しい FileId を採番し、ディスク上のカウンターを更新する
func (c *Catalog) AllocateFileId(bp *buffer.BufferPool) (page.FileId, error) {
	id := c.NextFileId
	c.NextFileId++

	// Header Page (Page 0) を更新する
	headerPageId := page.NewPageId(page.FileId(0), 0)
	headerPage, err := bp.FetchPage(headerPageId)
	if err != nil {
		return 0, err
	}
	defer bp.UnRefPage(headerPageId)

	// 次に割り当てる FileId をヘッダーページの指定オフセットに書き込む
	data := headerPage.GetWriteData()
	binary.BigEndian.PutUint32(data[16:20], uint32(c.NextFileId))

	return id, nil
}
