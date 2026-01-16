package catalog

import (
	"encoding/binary"
	"minesql/internal/storage/access/btree"
	"minesql/internal/storage/access/btree/node"
	"minesql/internal/storage/access/table"
	"minesql/internal/storage/bufferpool"
	"minesql/internal/storage/page"
)

type Catalog struct {
	TableMetaPageId  page.PageId
	IndexMetaPageId  page.PageId
	ColumnMetaPageId page.PageId
	metadata         []TableMetadata
}

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

	// 値をエンコード (Name, NCols)
	var encodedValue []byte
	valBuf := binary.BigEndian.AppendUint64(nil, uint64(tableMeta.NCols))
	table.Encode([][]byte{[]byte(tableMeta.Name), valBuf}, &encodedValue)

	// B+Tree に挿入
	return btr.Insert(bpm, node.NewPair(encodedKey, encodedValue))
}

func (c *Catalog) insertColumnMetadata(bpm *bufferpool.BufferPoolManager, columnMeta ColumnMetadata) error {
	btr := btree.NewBTree(c.ColumnMetaPageId)

	// キーをエンコード (TableId)
	var encodedKey []byte
	keyBuf := binary.BigEndian.AppendUint64(nil, columnMeta.TableId)
	table.Encode([][]byte{keyBuf}, &encodedKey)

	// 値をエンコード (Name, Pos, Type)
	var encodedValue []byte
	posBuf := binary.BigEndian.AppendUint16(nil, columnMeta.Pos)
	table.Encode([][]byte{[]byte(columnMeta.Name), posBuf, []byte(columnMeta.Type)}, &encodedValue)

	// B+Tree に挿入
	return btr.Insert(bpm, node.NewPair(encodedKey, encodedValue))
}

func (c *Catalog) insertIndexMetadata(bpm *bufferpool.BufferPoolManager, indexMeta IndexMetadata) error {
	btr := btree.NewBTree(c.IndexMetaPageId)

	// キーをエンコード (TableId)
	var encodedKey []byte
	keyBuf := binary.BigEndian.AppendUint64(nil, indexMeta.TableId)
	table.Encode([][]byte{keyBuf}, &encodedKey)

	// 値をエンコード (Name, Type)
	var encodedValue []byte
	table.Encode([][]byte{[]byte(indexMeta.Name), []byte(indexMeta.Type)}, &encodedValue)

	// B+Tree に挿入
	return btr.Insert(bpm, node.NewPair(encodedKey, encodedValue))
}
