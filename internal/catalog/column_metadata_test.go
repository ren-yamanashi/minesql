package catalog

import (
	"encoding/binary"
	"minesql/internal/btree"
	"minesql/internal/encode"
	"minesql/internal/storage"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestColumnMetadata_Insert(t *testing.T) {
	t.Run("カラムメタデータを B+Tree に挿入できる", func(t *testing.T) {
		// GIVEN
		bp, tmpdir := InitCatalogDisk(t)
		defer removeTmpdir(t, tmpdir)

		metaPageId, err := bp.AllocatePageId(storage.FileId(0))
		assert.NoError(t, err)
		_, err = btree.CreateBPlusTree(bp, metaPageId)
		assert.NoError(t, err)

		colMeta := NewColumnMetadata(1, "email", 2, ColumnTypeString)
		colMeta.MetaPageId = metaPageId

		// WHEN
		err = colMeta.Insert(bp)

		// THEN: B+Tree から挿入したデータを検索して確認
		assert.NoError(t, err)

		btr := btree.NewBPlusTree(metaPageId)
		iter, err := btr.Search(bp, btree.SearchModeStart{})
		assert.NoError(t, err)

		record, ok := iter.Get()
		assert.True(t, ok)

		// key (FileId, ColName) をデコード
		var keyParts [][]byte
		encode.Decode(record.KeyBytes(), &keyParts)
		tableId := binary.BigEndian.Uint32(keyParts[0])
		assert.Equal(t, uint32(1), tableId)
		assert.Equal(t, "email", string(keyParts[1]))

		// value (Pos, Type) をデコード
		var valueParts [][]byte
		encode.Decode(record.NonKeyBytes(), &valueParts)
		pos := binary.BigEndian.Uint16(valueParts[0])
		assert.Equal(t, uint16(2), pos)
		assert.Equal(t, string(ColumnTypeString), string(valueParts[1]))
	})

	t.Run("同じテーブルの複数カラムを挿入できる", func(t *testing.T) {
		// GIVEN
		bp, tmpdir := InitCatalogDisk(t)
		defer removeTmpdir(t, tmpdir)

		metaPageId, err := bp.AllocatePageId(storage.FileId(0))
		assert.NoError(t, err)
		_, err = btree.CreateBPlusTree(bp, metaPageId)
		assert.NoError(t, err)

		cols := []*ColumnMetadata{
			NewColumnMetadata(1, "id", 0, ColumnTypeString),
			NewColumnMetadata(1, "name", 1, ColumnTypeString),
			NewColumnMetadata(1, "email", 2, ColumnTypeString),
		}

		// WHEN
		for _, col := range cols {
			col.MetaPageId = metaPageId
			err = col.Insert(bp)
			assert.NoError(t, err)
		}

		// THEN: 3 件挿入されている
		btr := btree.NewBPlusTree(metaPageId)
		iter, err := btr.Search(bp, btree.SearchModeStart{})
		assert.NoError(t, err)

		count := 0
		for {
			_, ok := iter.Get()
			if !ok {
				break
			}
			count++
			err = iter.Advance(bp)
			assert.NoError(t, err)
		}
		assert.Equal(t, 3, count)
	})
}

func TestLoadColumnMetadata(t *testing.T) {
	t.Run("指定したテーブルのカラムメタデータを読み込める", func(t *testing.T) {
		// GIVEN
		bp, tmpdir := InitCatalogDisk(t)
		defer removeTmpdir(t, tmpdir)

		cat, err := CreateCatalog(bp)
		assert.NoError(t, err)

		// カラムメタデータを B+Tree に挿入
		cols := []*ColumnMetadata{
			NewColumnMetadata(1, "id", 0, ColumnTypeString),
			NewColumnMetadata(1, "name", 1, ColumnTypeString),
			NewColumnMetadata(1, "email", 2, ColumnTypeString),
		}
		for _, col := range cols {
			col.MetaPageId = cat.ColumnMetaPageId
			err = col.Insert(bp)
			assert.NoError(t, err)
		}

		// WHEN
		result, err := loadColumnMetadata(bp, 1, cat.ColumnMetaPageId)

		// THEN
		assert.NoError(t, err)
		assert.Equal(t, 3, len(result))
		// Pos 順にソートされている
		assert.Equal(t, "id", result[0].Name)
		assert.Equal(t, uint16(0), result[0].Pos)
		assert.Equal(t, "name", result[1].Name)
		assert.Equal(t, uint16(1), result[1].Pos)
		assert.Equal(t, "email", result[2].Name)
		assert.Equal(t, uint16(2), result[2].Pos)
	})

	t.Run("B+Tree のキー順ではなく Pos 順にソートされる", func(t *testing.T) {
		// GIVEN
		bp, tmpdir := InitCatalogDisk(t)
		defer removeTmpdir(t, tmpdir)

		cat, err := CreateCatalog(bp)
		assert.NoError(t, err)

		// Pos が逆順になるようなカラム名で挿入 (B+Tree は "z_col" < "a_col" の順にはならない)
		cols := []*ColumnMetadata{
			NewColumnMetadata(1, "z_col", 0, ColumnTypeString),
			NewColumnMetadata(1, "a_col", 1, ColumnTypeString),
			NewColumnMetadata(1, "m_col", 2, ColumnTypeString),
		}
		for _, col := range cols {
			col.MetaPageId = cat.ColumnMetaPageId
			err = col.Insert(bp)
			assert.NoError(t, err)
		}

		// WHEN
		result, err := loadColumnMetadata(bp, 1, cat.ColumnMetaPageId)

		// THEN: B+Tree のキー順 (a_col, m_col, z_col) ではなく Pos 順 (z_col, a_col, m_col)
		assert.NoError(t, err)
		assert.Equal(t, 3, len(result))
		assert.Equal(t, "z_col", result[0].Name)
		assert.Equal(t, "a_col", result[1].Name)
		assert.Equal(t, "m_col", result[2].Name)
	})

	t.Run("指定したテーブルのカラムのみ取得される", func(t *testing.T) {
		// GIVEN
		bp, tmpdir := InitCatalogDisk(t)
		defer removeTmpdir(t, tmpdir)

		cat, err := CreateCatalog(bp)
		assert.NoError(t, err)

		// テーブル 1 と テーブル 2 のカラムを混在させて挿入
		cols := []*ColumnMetadata{
			NewColumnMetadata(1, "id", 0, ColumnTypeString),
			NewColumnMetadata(2, "id", 0, ColumnTypeString),
			NewColumnMetadata(1, "name", 1, ColumnTypeString),
			NewColumnMetadata(2, "title", 1, ColumnTypeString),
		}
		for _, col := range cols {
			col.MetaPageId = cat.ColumnMetaPageId
			err = col.Insert(bp)
			assert.NoError(t, err)
		}

		// WHEN
		result, err := loadColumnMetadata(bp, 1, cat.ColumnMetaPageId)

		// THEN: テーブル 1 のカラムのみ取得される
		assert.NoError(t, err)
		assert.Equal(t, 2, len(result))
		assert.Equal(t, "id", result[0].Name)
		assert.Equal(t, "name", result[1].Name)
		assert.Equal(t, storage.FileId(1), result[0].FileId)
		assert.Equal(t, storage.FileId(1), result[1].FileId)
	})

	t.Run("該当するカラムがない場合は空のスライスを返す", func(t *testing.T) {
		// GIVEN
		bp, tmpdir := InitCatalogDisk(t)
		defer removeTmpdir(t, tmpdir)

		cat, err := CreateCatalog(bp)
		assert.NoError(t, err)

		// WHEN
		result, err := loadColumnMetadata(bp, 999, cat.ColumnMetaPageId)

		// THEN
		assert.NoError(t, err)
		assert.Empty(t, result)
	})
}
