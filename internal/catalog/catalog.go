package catalog

import (
	"encoding/binary"
	"fmt"
	"minesql/internal/btree"
	"minesql/internal/storage"
)

var (
	ErrInvalidCatalogFile = fmt.Errorf("invalid database catalog file: magic number mismatch")
)

// Catalog はテーブルのメタデータ (テーブル情報、インデックス情報、カラム情報) を管理する
type Catalog struct {
	TableMetaPageId  storage.PageId
	IndexMetaPageId  storage.PageId
	ColumnMetaPageId storage.PageId
	metadata         []*TableMetadata
	NextFileId       storage.FileId
}

// NewCatalog は既存のカタログを開く
func NewCatalog(bp *storage.BufferPool) (*Catalog, error) {
	fileId := storage.FileId(0) // カタログ専用の FileId を使用
	headerPageId := storage.NewPageId(fileId, storage.PageNumber(0))

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
	nextFileId := storage.FileId(binary.BigEndian.Uint32(data[16:20]))

	catalog := &Catalog{
		TableMetaPageId:  storage.NewPageId(fileId, storage.PageNumber(tblMetaPageNum)),
		IndexMetaPageId:  storage.NewPageId(fileId, storage.PageNumber(idxMetaPageNum)),
		ColumnMetaPageId: storage.NewPageId(fileId, storage.PageNumber(colMetaPageNum)),
		metadata:         nil,
		NextFileId:       nextFileId,
	}

	// ディスクから既存のメタデータを読み込む
	tableMeta, err := loadTableMetadata(bp, catalog.TableMetaPageId, catalog.IndexMetaPageId, catalog.ColumnMetaPageId)
	if err != nil {
		return nil, err
	}
	catalog.metadata = tableMeta

	return catalog, nil
}

// CreateCatalog はカタログを新規作成する
func CreateCatalog(bp *storage.BufferPool) (*Catalog, error) {
	fileId := storage.FileId(0) // カタログ専用の FileId を使用

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
	tblMetaTree, err := btree.CreateBPlusTree(bp, tblMetaPageId)
	if err != nil {
		return nil, err
	}

	// インデックスメタデータ用の B+Tree を作成
	idxMetaPageId, err := bp.AllocatePageId(fileId)
	if err != nil {
		return nil, err
	}
	idxMetaTree, err := btree.CreateBPlusTree(bp, idxMetaPageId)
	if err != nil {
		return nil, err
	}

	// カラムメタデータ用の B+Tree を作成
	colMetaPageId, err := bp.AllocatePageId(fileId)
	if err != nil {
		return nil, err
	}
	colMetaTree, err := btree.CreateBPlusTree(bp, colMetaPageId)
	if err != nil {
		return nil, err
	}

	// ヘッダーページに各メタデータのメタページIDを保存
	data := bufferPage.GetWriteData()
	initFileId := storage.FileId(1) // FileId(0) はカタログ用に予約されているため、1 から開始
	copy(data[0:4], []byte("MINE")) // ファイルシグネチャとしてマジックナンバーを設定 (minesql なので MINE)
	binary.BigEndian.PutUint32(data[4:8], uint32(tblMetaTree.MetaPageId.PageNumber))
	binary.BigEndian.PutUint32(data[8:12], uint32(idxMetaTree.MetaPageId.PageNumber))
	binary.BigEndian.PutUint32(data[12:16], uint32(colMetaTree.MetaPageId.PageNumber))
	binary.BigEndian.PutUint32(data[16:20], uint32(initFileId))

	return &Catalog{
		TableMetaPageId:  tblMetaTree.MetaPageId,
		ColumnMetaPageId: colMetaTree.MetaPageId,
		IndexMetaPageId:  idxMetaTree.MetaPageId,
		NextFileId:       initFileId,
		metadata:         nil,
	}, nil
}

// Insert はカタログにメタデータを挿入する
func (c *Catalog) Insert(bp *storage.BufferPool, tableMeta TableMetadata) error {
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

// GetTableMetadataByName はテーブル名からテーブルメタデータを取得する
func (c *Catalog) GetTableMetadataByName(tableName string) (*TableMetadata, bool) {
	for _, tblMeta := range c.metadata {
		if tblMeta.Name == tableName {
			return tblMeta, true
		}
	}
	return nil, false
}

// GetAllTables はすべてのテーブルメタデータを取得する
func (c *Catalog) GetAllTables() []*TableMetadata {
	return c.metadata
}

// AllocateFileId は新しい FileId を採番し、ディスク上のカウンターを更新する
func (c *Catalog) AllocateFileId(bp *storage.BufferPool) (storage.FileId, error) {
	id := c.NextFileId
	c.NextFileId++

	// Header Page (Page 0) を更新する
	headerPageId := storage.NewPageId(storage.FileId(0), 0)
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
