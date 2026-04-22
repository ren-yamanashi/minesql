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

// Catalog はテーブルのメタデータ (テーブル情報、インデックス情報、カラム情報、制約情報) を管理する
type Catalog struct {
	TableMetaPageId      page.PageId
	IndexMetaPageId      page.PageId
	ColumnMetaPageId     page.PageId
	ConstraintMetaPageId page.PageId
	NextFileId           page.FileId
	UndoFileId           page.FileId
	metadata             []*TableMeta
}

// NewCatalog は既存のカタログを開く
func NewCatalog(bp *buffer.BufferPool) (*Catalog, error) {
	fileId := page.FileId(0) // カタログ専用の FileId を使用
	headerPageId := page.NewPageId(fileId, page.PageNumber(0))

	// ヘッダーページを読み込む
	data, err := bp.GetReadPageData(headerPageId)
	if err != nil {
		return nil, err
	}
	defer bp.UnRefPage(headerPageId)
	if string(data[0:4]) != "MINE" {
		return nil, ErrInvalidCatalogFile
	}

	// 各メタデータの MetaPageId を取得
	tblMetaPageNum := binary.BigEndian.Uint32(data[4:8])
	idxMetaPageNum := binary.BigEndian.Uint32(data[8:12])
	colMetaPageNum := binary.BigEndian.Uint32(data[12:16])
	conMetaPageNum := binary.BigEndian.Uint32(data[16:20])
	nextFileId := page.FileId(binary.BigEndian.Uint32(data[20:24]))
	undoFileId := page.FileId(binary.BigEndian.Uint32(data[24:28]))

	catalog := &Catalog{
		TableMetaPageId:      page.NewPageId(fileId, page.PageNumber(tblMetaPageNum)),
		IndexMetaPageId:      page.NewPageId(fileId, page.PageNumber(idxMetaPageNum)),
		ColumnMetaPageId:     page.NewPageId(fileId, page.PageNumber(colMetaPageNum)),
		ConstraintMetaPageId: page.NewPageId(fileId, page.PageNumber(conMetaPageNum)),
		NextFileId:           nextFileId,
		UndoFileId:           undoFileId,
		metadata:             nil,
	}

	// ディスクから既存のメタデータを読み込む
	tableMeta, err := loadTableMeta(bp, catalog.TableMetaPageId, catalog.IndexMetaPageId, catalog.ColumnMetaPageId, catalog.ConstraintMetaPageId)
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
	err = bp.AddPage(headerPageId)
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

	// 制約メタデータ用の B+Tree を作成
	conMetaPageId, err := bp.AllocatePageId(fileId)
	if err != nil {
		return nil, err
	}
	conMetaTree, err := btree.CreateBTree(bp, conMetaPageId)
	if err != nil {
		return nil, err
	}

	// ヘッダーページに各メタデータのメタページIDを保存
	data, err := bp.GetWritePageData(headerPageId)
	if err != nil {
		return nil, err
	}

	nextFileId := page.FileId(1) // FileId(0) はカタログ用なので 1 から開始
	undoFileId := nextFileId     // UNDO ログ用の FileId を採番
	nextFileId++
	copy(data[0:4], []byte("MINE")) // ファイルシグネチャとしてマジックナンバーを設定 (minesql なので MINE)
	binary.BigEndian.PutUint32(data[4:8], uint32(tblMetaTree.MetaPageId.PageNumber))
	binary.BigEndian.PutUint32(data[8:12], uint32(idxMetaTree.MetaPageId.PageNumber))
	binary.BigEndian.PutUint32(data[12:16], uint32(colMetaTree.MetaPageId.PageNumber))
	binary.BigEndian.PutUint32(data[16:20], uint32(conMetaTree.MetaPageId.PageNumber))
	binary.BigEndian.PutUint32(data[20:24], uint32(nextFileId))
	binary.BigEndian.PutUint32(data[24:28], uint32(undoFileId))

	return &Catalog{
		TableMetaPageId:      tblMetaTree.MetaPageId,
		IndexMetaPageId:      idxMetaTree.MetaPageId,
		ColumnMetaPageId:     colMetaTree.MetaPageId,
		ConstraintMetaPageId: conMetaTree.MetaPageId,
		NextFileId:           nextFileId,
		UndoFileId:           undoFileId,
		metadata:             nil,
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
	for _, conMeta := range tableMeta.Constraints {
		conMeta.MetaPageId = c.ConstraintMetaPageId
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

	// 制約メタデータを挿入
	for _, conMeta := range tableMeta.Constraints {
		if err := conMeta.Insert(bp); err != nil {
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

// ChildForeignKey は参照元テーブルの FK 制約と、その FK が属するテーブル名を組み合わせたもの
type ChildForeignKey struct {
	TableName  string          // FK 制約が属するテーブル名 (子テーブル名)
	Constraint *ConstraintMeta // FK 制約メタデータ
}

// GetForeignKeysReferencingTable は指定されたテーブルを参照する FK 制約を全テーブルから収集する
//
// 計算量は O(テーブル数 x 制約数) の全走査。学習プロジェクトのため許容する。
// 将来的に逆引きインデックスの追加で最適化可能。
func (c *Catalog) GetForeignKeysReferencingTable(tableName string) []ChildForeignKey {
	var result []ChildForeignKey
	for _, tblMeta := range c.metadata {
		for _, con := range tblMeta.Constraints {
			if con.RefTableName == tableName {
				result = append(result, ChildForeignKey{
					TableName:  tblMeta.Name,
					Constraint: con,
				})
			}
		}
	}
	return result
}

// AllocateFileId は新しい FileId を採番し、ディスク上のカウンターを更新する
func (c *Catalog) AllocateFileId(bp *buffer.BufferPool) (page.FileId, error) {
	id := c.NextFileId
	c.NextFileId++

	// Header Page (Page 0) を更新する
	headerPageId := page.NewPageId(page.FileId(0), 0)
	data, err := bp.GetWritePageData(headerPageId)
	if err != nil {
		return 0, err
	}
	defer bp.UnRefPage(headerPageId)

	// 次に割り当てる FileId をヘッダーページの指定オフセットに書き込む
	binary.BigEndian.PutUint32(data[20:24], uint32(c.NextFileId))

	return id, nil
}
