package catalog

import (
	"encoding/binary"
	"fmt"
	"minesql/internal/storage/access/btree"
	"minesql/internal/storage/access/btree/node"
	"minesql/internal/storage/access/table"
	"minesql/internal/storage/bufferpool"
	"minesql/internal/storage/page"
)

var (
	ErrInvalidCatalogFile = fmt.Errorf("invalid database catalog file: magic number mismatch")
)

type Catalog struct {
	TableMetaPageId  page.PageId
	IndexMetaPageId  page.PageId
	ColumnMetaPageId page.PageId
	metadata         []TableMetadata
	NextTableId      uint64
}

// 既存のカタログを開く
func NewCatalog(bpm *bufferpool.BufferPoolManager) (*Catalog, error) {
	fileId := page.FileId(0) // カタログ専用の FileId を使用
	headerPageId := page.NewPageId(fileId, page.PageNumber(0))

	// ヘッダーページを読み込む
	bufPage, err := bpm.FetchPage(headerPageId)
	if err != nil {
		return nil, err
	}
	defer bpm.UnRefPage(headerPageId)

	// データを読み取る
	data := bufPage.GetReadData()
	if string(data[0:4]) != "MINE" {
		return nil, ErrInvalidCatalogFile
	}

	// 各メタデータの MetaPageId を取得
	tblMetaPageNum := binary.BigEndian.Uint32(data[4:8])
	idxMetaPageNum := binary.BigEndian.Uint32(data[8:12])
	colMetaPageNum := binary.BigEndian.Uint32(data[12:16])
	initTableId := binary.BigEndian.Uint64(data[16:24])

	return &Catalog{
		TableMetaPageId:  page.NewPageId(fileId, page.PageNumber(tblMetaPageNum)),
		IndexMetaPageId:  page.NewPageId(fileId, page.PageNumber(idxMetaPageNum)),
		ColumnMetaPageId: page.NewPageId(fileId, page.PageNumber(colMetaPageNum)),
		metadata:         []TableMetadata{},
		NextTableId:      initTableId,
	}, nil
}

// カタログを新規作成する
func CreateCatalog(bpm *bufferpool.BufferPoolManager) (*Catalog, error) {
	fileId := page.FileId(0) // カタログ専用の FileId を使用

	// ヘッダーページを作成
	headerPageId, err := bpm.AllocatePageId(fileId)
	if err != nil {
		return nil, err
	}
	bufferPage, err := bpm.AddPage(headerPageId)
	if err != nil {
		return nil, err
	}
	defer bpm.UnRefPage(headerPageId)

	// テーブルメタデータ用の B+Tree を作成
	tblMetaPageId, err := bpm.AllocatePageId(fileId)
	if err != nil {
		return nil, err
	}
	tblMetaTree, err := btree.CreateBTree(bpm, tblMetaPageId)
	if err != nil {
		return nil, err
	}

	// カラムメタデータ用の B+Tree を作成
	colMetaPageId, err := bpm.AllocatePageId(fileId)
	if err != nil {
		return nil, err
	}
	colMetaTree, err := btree.CreateBTree(bpm, colMetaPageId)
	if err != nil {
		return nil, err
	}

	// インデックスメタデータ用の B+Tree を作成
	idxMetaPageId, err := bpm.AllocatePageId(fileId)
	if err != nil {
		return nil, err
	}
	idxMetaTree, err := btree.CreateBTree(bpm, idxMetaPageId)
	if err != nil {
		return nil, err
	}

	// ヘッダーページに各メタデータのメタページIDを保存
	data := bufferPage.GetWriteData()
	initPageId := uint64(0)
	copy(data[0:4], []byte("MINE")) // ファイルシグネチャとしてマジックナンバーを設定 (minesql なので MINE)
	binary.BigEndian.PutUint32(data[4:8], uint32(tblMetaTree.MetaPageId.PageNumber))
	binary.BigEndian.PutUint32(data[8:12], uint32(idxMetaTree.MetaPageId.PageNumber))
	binary.BigEndian.PutUint32(data[12:16], uint32(colMetaTree.MetaPageId.PageNumber))
	binary.BigEndian.PutUint64(data[16:24], initPageId)

	return &Catalog{
		TableMetaPageId:  tblMetaTree.MetaPageId,
		ColumnMetaPageId: colMetaTree.MetaPageId,
		IndexMetaPageId:  idxMetaTree.MetaPageId,
		NextTableId:      initPageId,
		metadata:         []TableMetadata{},
	}, nil
}

// 新しい TableID を採番し、ディスク上のカウンターを更新する
func (c *Catalog) AllocateTableId(bpm *bufferpool.BufferPoolManager) (uint64, error) {
	id := c.NextTableId
	c.NextTableId++

	// Header Page (Page 0) を更新する
	fileId := page.FileId(0)
	headerPageId := page.NewPageId(fileId, 0)
	headerPage, err := bpm.FetchPage(headerPageId)
	if err != nil {
		return 0, err
	}
	defer bpm.UnRefPage(headerPageId)

	data := headerPage.GetWriteData()
	binary.BigEndian.PutUint64(data[16:24], c.NextTableId)

	return id, nil
}

// カタログにメタデータを挿入する
func (c *Catalog) Insert(bpm *bufferpool.BufferPoolManager, tableMeta TableMetadata) error {
	// テーブルメタデータを挿入
	if err := c.insertTableMetadata(bpm, tableMeta); err != nil {
		return err
	}

	// カラムメタデータを挿入
	for _, colMeta := range tableMeta.Cols {
		if err := c.insertColumnMetadata(bpm, colMeta); err != nil {
			return err
		}
	}

	// インデックスメタデータを挿入
	for _, indexMeta := range tableMeta.Indexes {
		if err := c.insertIndexMetadata(bpm, indexMeta); err != nil {
			return err
		}
	}

	c.metadata = append(c.metadata, tableMeta)
	return nil
}

func (c *Catalog) insertTableMetadata(bpm *bufferpool.BufferPoolManager, tableMeta TableMetadata) error {
	btr := btree.NewBTree(c.TableMetaPageId)

	// キーをエンコード (TableId)
	var encodedKey []byte
	keyBuf := binary.BigEndian.AppendUint64(nil, tableMeta.TableId)
	table.Encode([][]byte{keyBuf}, &encodedKey)

	// 値をエンコード (Name, NCols, DataMetaPageId)
	var encodedValue []byte
	valBuf := binary.BigEndian.AppendUint64(nil, uint64(tableMeta.NCols))
	table.Encode([][]byte{[]byte(tableMeta.Name), valBuf, tableMeta.DataMetaPageId.ToBytes()}, &encodedValue)

	// B+Tree に挿入
	return btr.Insert(bpm, node.NewPair(encodedKey, encodedValue))
}

func (c *Catalog) insertColumnMetadata(bpm *bufferpool.BufferPoolManager, columnMeta *ColumnMetadata) error {
	btr := btree.NewBTree(c.ColumnMetaPageId)

	// キーをエンコード (TableId, ColName)
	var encodedKey []byte
	keyBuf := binary.BigEndian.AppendUint64(nil, columnMeta.TableId)
	table.Encode([][]byte{keyBuf, []byte(columnMeta.Name)}, &encodedKey)

	// 値をエンコード (Pos, Type)
	var encodedValue []byte
	posBuf := binary.BigEndian.AppendUint16(nil, columnMeta.Pos)
	table.Encode([][]byte{posBuf, []byte(columnMeta.Type)}, &encodedValue)

	// B+Tree に挿入
	return btr.Insert(bpm, node.NewPair(encodedKey, encodedValue))
}

func (c *Catalog) insertIndexMetadata(bpm *bufferpool.BufferPoolManager, indexMeta *IndexMetadata) error {
	btr := btree.NewBTree(c.IndexMetaPageId)

	// キーをエンコード (TableId, Name)
	var encodedKey []byte
	keyBuf := binary.BigEndian.AppendUint64(nil, indexMeta.TableId)
	table.Encode([][]byte{keyBuf, []byte(indexMeta.Name)}, &encodedKey)

	// 値をエンコード (Type, DataMetaPageId)
	var encodedValue []byte
	table.Encode([][]byte{[]byte(indexMeta.Type), indexMeta.DataMetaPageId.ToBytes()}, &encodedValue)

	// B+Tree に挿入
	return btr.Insert(bpm, node.NewPair(encodedKey, encodedValue))
}

// テーブル名からテーブルメタデータを取得する
func (c *Catalog) GetTableMetadataByName(tableName string) (*TableMetadata, error) {
	for _, tblMeta := range c.metadata {
		if tblMeta.Name == tableName {
			return &tblMeta, nil
		}
	}
	return nil, fmt.Errorf("table %s not found in catalog", tableName)
}
