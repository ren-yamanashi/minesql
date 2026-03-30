package dictionary

import (
	"encoding/binary"
	"minesql/internal/storage/btree"
	"minesql/internal/storage/encode"
	"minesql/internal/storage/page"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestIndexMetadata_Insert(t *testing.T) {
	t.Run("インデックスメタデータを B+Tree に挿入できる", func(t *testing.T) {
		// GIVEN
		bp, tmpdir := InitCatalogDisk(t)
		defer removeTmpdir(t, tmpdir)

		// インデックスメタデータ用の B+Tree を作成
		metaPageId, err := bp.AllocatePageId(page.FileId(0))
		assert.NoError(t, err)
		_, err = btree.CreateBTree(bp, metaPageId)
		assert.NoError(t, err)

		dataMetaPageId := page.NewPageId(page.FileId(1), 5)
		idxMeta := NewIndexMetadata(1, "idx_email", "email", IndexTypeUnique, dataMetaPageId)
		idxMeta.MetaPageId = metaPageId

		// WHEN
		err = idxMeta.Insert(bp)

		// THEN: B+Tree から挿入したデータを検索して確認
		assert.NoError(t, err)

		btr := btree.NewBTree(metaPageId)
		iter, err := btr.Search(bp, btree.SearchModeStart{})
		assert.NoError(t, err)

		record, ok := iter.Get()
		assert.True(t, ok)

		// key (FileId, Name) をデコード
		var keyParts [][]byte
		encode.Decode(record.KeyBytes(), &keyParts)
		tableId := binary.BigEndian.Uint32(keyParts[0])
		assert.Equal(t, uint32(1), tableId)
		assert.Equal(t, "idx_email", string(keyParts[1]))

		// value (Type, ColName, DataMetaPageId) をデコード
		var valueParts [][]byte
		encode.Decode(record.NonKeyBytes(), &valueParts)
		assert.Equal(t, string(IndexTypeUnique), string(valueParts[0]))
		assert.Equal(t, "email", string(valueParts[1]))
		assert.Equal(t, dataMetaPageId, page.RestorePageIdFromBytes(valueParts[2]))
	})

	t.Run("異なるテーブルのインデックスを挿入できる", func(t *testing.T) {
		// GIVEN
		bp, tmpdir := InitCatalogDisk(t)
		defer removeTmpdir(t, tmpdir)

		metaPageId, err := bp.AllocatePageId(page.FileId(0))
		assert.NoError(t, err)
		_, err = btree.CreateBTree(bp, metaPageId)
		assert.NoError(t, err)

		idx1 := NewIndexMetadata(1, "idx_email", "email", IndexTypeUnique, page.NewPageId(page.FileId(1), 5))
		idx1.MetaPageId = metaPageId
		idx2 := NewIndexMetadata(2, "idx_title", "title", IndexTypeUnique, page.NewPageId(page.FileId(2), 6))
		idx2.MetaPageId = metaPageId

		// WHEN
		err = idx1.Insert(bp)
		assert.NoError(t, err)
		err = idx2.Insert(bp)
		assert.NoError(t, err)

		// THEN: 2 件挿入されている
		btr := btree.NewBTree(metaPageId)
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
		assert.Equal(t, 2, count)
	})

	t.Run("同じテーブルの複数のインデックスを挿入できる", func(t *testing.T) {
		// GIVEN
		bp, tmpdir := InitCatalogDisk(t)
		defer removeTmpdir(t, tmpdir)

		metaPageId, err := bp.AllocatePageId(page.FileId(0))
		assert.NoError(t, err)
		_, err = btree.CreateBTree(bp, metaPageId)
		assert.NoError(t, err)

		idx1 := NewIndexMetadata(1, "idx_email", "email", IndexTypeUnique, page.NewPageId(page.FileId(1), 5))
		idx1.MetaPageId = metaPageId
		idx2 := NewIndexMetadata(1, "idx_username", "username", IndexTypeUnique, page.NewPageId(page.FileId(1), 6))
		idx2.MetaPageId = metaPageId

		// WHEN
		err = idx1.Insert(bp)
		assert.NoError(t, err)
		err = idx2.Insert(bp)
		assert.NoError(t, err)

		// THEN: 2 件挿入されている
		btr := btree.NewBTree(metaPageId)
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
		assert.Equal(t, 2, count)
	})
}

func TestLoadIndexMetadata(t *testing.T) {
	t.Run("指定したテーブルのインデックスメタデータを読み込める", func(t *testing.T) {
		// GIVEN
		bp, tmpdir := InitCatalogDisk(t)
		defer removeTmpdir(t, tmpdir)

		cat, err := CreateCatalog(bp)
		assert.NoError(t, err)

		dataMetaPageId1 := page.NewPageId(page.FileId(1), 5)
		dataMetaPageId2 := page.NewPageId(page.FileId(1), 6)
		indexes := []*IndexMetadata{
			NewIndexMetadata(1, "idx_email", "email", IndexTypeUnique, dataMetaPageId1),
			NewIndexMetadata(1, "idx_username", "username", IndexTypeUnique, dataMetaPageId2),
		}
		for _, idx := range indexes {
			idx.MetaPageId = cat.IndexMetaPageId
			err = idx.Insert(bp)
			assert.NoError(t, err)
		}

		// WHEN
		result, err := loadIndexMetadata(bp, 1, cat.IndexMetaPageId)

		// THEN
		assert.NoError(t, err)
		assert.Equal(t, 2, len(result))
		assert.Equal(t, "idx_email", result[0].Name)
		assert.Equal(t, "email", result[0].ColName)
		assert.Equal(t, IndexTypeUnique, result[0].Type)
		assert.Equal(t, dataMetaPageId1, result[0].DataMetaPageId)
		assert.Equal(t, "idx_username", result[1].Name)
		assert.Equal(t, "username", result[1].ColName)
		assert.Equal(t, dataMetaPageId2, result[1].DataMetaPageId)
	})

	t.Run("指定したテーブルのインデックスのみ取得される", func(t *testing.T) {
		// GIVEN
		bp, tmpdir := InitCatalogDisk(t)
		defer removeTmpdir(t, tmpdir)

		cat, err := CreateCatalog(bp)
		assert.NoError(t, err)

		// テーブル 1 と テーブル 2 のインデックスを混在させて挿入
		indexes := []*IndexMetadata{
			NewIndexMetadata(1, "idx_email", "email", IndexTypeUnique, page.NewPageId(page.FileId(1), 5)),
			NewIndexMetadata(2, "idx_title", "title", IndexTypeUnique, page.NewPageId(page.FileId(2), 6)),
			NewIndexMetadata(1, "idx_username", "username", IndexTypeUnique, page.NewPageId(page.FileId(1), 7)),
		}
		for _, idx := range indexes {
			idx.MetaPageId = cat.IndexMetaPageId
			err = idx.Insert(bp)
			assert.NoError(t, err)
		}

		// WHEN
		result, err := loadIndexMetadata(bp, 1, cat.IndexMetaPageId)

		// THEN: テーブル 1 のインデックスのみ取得される
		assert.NoError(t, err)
		assert.Equal(t, 2, len(result))
		assert.Equal(t, page.FileId(1), result[0].FileId)
		assert.Equal(t, page.FileId(1), result[1].FileId)
	})

	t.Run("該当するインデックスがない場合は空のスライスを返す", func(t *testing.T) {
		// GIVEN
		bp, tmpdir := InitCatalogDisk(t)
		defer removeTmpdir(t, tmpdir)

		cat, err := CreateCatalog(bp)
		assert.NoError(t, err)

		// WHEN
		result, err := loadIndexMetadata(bp, 999, cat.IndexMetaPageId)

		// THEN
		assert.NoError(t, err)
		assert.Empty(t, result)
	})
}
