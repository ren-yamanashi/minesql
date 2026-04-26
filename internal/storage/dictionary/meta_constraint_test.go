package dictionary

import (
	"encoding/binary"
	"testing"

	"github.com/ren-yamanashi/minesql/internal/storage/btree"
	"github.com/ren-yamanashi/minesql/internal/storage/encode"
	"github.com/ren-yamanashi/minesql/internal/storage/page"
	"github.com/stretchr/testify/assert"
)

func TestConstraintMeta_Insert(t *testing.T) {
	t.Run("制約メタデータを B+Tree に挿入できる", func(t *testing.T) {
		// GIVEN
		bp, tmpdir := InitCatalogDisk(t)
		defer removeTmpdir(t, tmpdir)

		metaPageId, err := bp.AllocatePageId(page.FileId(0))
		assert.NoError(t, err)
		_, err = btree.CreateBTree(bp, metaPageId)
		assert.NoError(t, err)

		conMeta := NewConstraintMeta(1, "user_id", "fk_user", "users", "id")
		conMeta.MetaPageId = metaPageId

		// WHEN
		err = conMeta.Insert(bp)

		// THEN: B+Tree から挿入したデータを検索して確認
		assert.NoError(t, err)

		btr := btree.NewBTree(metaPageId)
		iter, err := btr.Search(bp, btree.SearchModeStart{})
		assert.NoError(t, err)

		record, ok := iter.Get()
		assert.True(t, ok)

		// key (FileId, ColName, ConstraintName) をデコード
		var keyParts [][]byte
		encode.Decode(record.KeyBytes(), &keyParts)
		fileId := binary.BigEndian.Uint32(keyParts[0])
		assert.Equal(t, uint32(1), fileId)
		assert.Equal(t, "user_id", string(keyParts[1]))
		assert.Equal(t, "fk_user", string(keyParts[2]))

		// value (フラグ, RefTableName, RefColName) をデコード
		var valueParts [][]byte
		encode.Decode(record.NonKeyBytes(), &valueParts)
		assert.Equal(t, byte(1), valueParts[0][0]) // 参照先あり
		assert.Equal(t, "users", string(valueParts[1]))
		assert.Equal(t, "id", string(valueParts[2]))
	})

	t.Run("同じカラムに複数の制約を挿入できる", func(t *testing.T) {
		// GIVEN
		bp, tmpdir := InitCatalogDisk(t)
		defer removeTmpdir(t, tmpdir)

		metaPageId, err := bp.AllocatePageId(page.FileId(0))
		assert.NoError(t, err)
		_, err = btree.CreateBTree(bp, metaPageId)
		assert.NoError(t, err)

		// 同じカラム (id) に PK と FK の 2 つの制約
		constraints := []*ConstraintMeta{
			NewConstraintMeta(1, "id", "PRIMARY", "", ""),
			NewConstraintMeta(1, "id", "fk_parent", "parents", "id"),
		}

		// WHEN
		for _, con := range constraints {
			con.MetaPageId = metaPageId
			err = con.Insert(bp)
			assert.NoError(t, err)
		}

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

	t.Run("主キーの制約メタデータを挿入できる", func(t *testing.T) {
		// GIVEN
		bp, tmpdir := InitCatalogDisk(t)
		defer removeTmpdir(t, tmpdir)

		metaPageId, err := bp.AllocatePageId(page.FileId(0))
		assert.NoError(t, err)
		_, err = btree.CreateBTree(bp, metaPageId)
		assert.NoError(t, err)

		conMeta := NewConstraintMeta(1, "id", "PRIMARY", "", "")
		conMeta.MetaPageId = metaPageId

		// WHEN
		err = conMeta.Insert(bp)

		// THEN
		assert.NoError(t, err)

		btr := btree.NewBTree(metaPageId)
		iter, err := btr.Search(bp, btree.SearchModeStart{})
		assert.NoError(t, err)

		record, ok := iter.Get()
		assert.True(t, ok)

		var valueParts [][]byte
		encode.Decode(record.NonKeyBytes(), &valueParts)
		assert.Equal(t, byte(0), valueParts[0][0]) // 参照先なし
		assert.Equal(t, 1, len(valueParts))        // フラグのみ
	})
}

func TestLoadConstraintMeta(t *testing.T) {
	t.Run("指定したテーブルの制約メタデータを読み込める", func(t *testing.T) {
		// GIVEN
		bp, tmpdir := InitCatalogDisk(t)
		defer removeTmpdir(t, tmpdir)

		cat, err := CreateCatalog(bp)
		assert.NoError(t, err)

		constraints := []*ConstraintMeta{
			NewConstraintMeta(1, "id", "PRIMARY", "", ""),
			NewConstraintMeta(1, "user_id", "fk_user", "users", "id"),
		}
		for _, con := range constraints {
			con.MetaPageId = cat.ConstraintMetaPageId
			err = con.Insert(bp)
			assert.NoError(t, err)
		}

		// WHEN
		result, err := loadConstraintMeta(bp, 1, cat.ConstraintMetaPageId)

		// THEN
		assert.NoError(t, err)
		assert.Equal(t, 2, len(result))
	})

	t.Run("指定したテーブルの制約のみ取得される", func(t *testing.T) {
		// GIVEN
		bp, tmpdir := InitCatalogDisk(t)
		defer removeTmpdir(t, tmpdir)

		cat, err := CreateCatalog(bp)
		assert.NoError(t, err)

		constraints := []*ConstraintMeta{
			NewConstraintMeta(1, "id", "PRIMARY", "", ""),
			NewConstraintMeta(2, "id", "PRIMARY", "", ""),
			NewConstraintMeta(1, "user_id", "fk_user", "users", "id"),
		}
		for _, con := range constraints {
			con.MetaPageId = cat.ConstraintMetaPageId
			err = con.Insert(bp)
			assert.NoError(t, err)
		}

		// WHEN
		result, err := loadConstraintMeta(bp, 1, cat.ConstraintMetaPageId)

		// THEN: テーブル 1 の制約のみ取得される
		assert.NoError(t, err)
		assert.Equal(t, 2, len(result))
		for _, con := range result {
			assert.Equal(t, page.FileId(1), con.FileId)
		}
	})

	t.Run("FK 制約の参照先情報が正しく読み込める", func(t *testing.T) {
		// GIVEN
		bp, tmpdir := InitCatalogDisk(t)
		defer removeTmpdir(t, tmpdir)

		cat, err := CreateCatalog(bp)
		assert.NoError(t, err)

		con := NewConstraintMeta(1, "user_id", "fk_user", "users", "id")
		con.MetaPageId = cat.ConstraintMetaPageId
		err = con.Insert(bp)
		assert.NoError(t, err)

		// WHEN
		result, err := loadConstraintMeta(bp, 1, cat.ConstraintMetaPageId)

		// THEN
		assert.NoError(t, err)
		assert.Equal(t, 1, len(result))
		assert.Equal(t, "user_id", result[0].ColName)
		assert.Equal(t, "fk_user", result[0].ConstraintName)
		assert.Equal(t, "users", result[0].RefTableName)
		assert.Equal(t, "id", result[0].RefColName)
	})

	t.Run("該当する制約がない場合は空のスライスを返す", func(t *testing.T) {
		// GIVEN
		bp, tmpdir := InitCatalogDisk(t)
		defer removeTmpdir(t, tmpdir)

		cat, err := CreateCatalog(bp)
		assert.NoError(t, err)

		// WHEN
		result, err := loadConstraintMeta(bp, 999, cat.ConstraintMetaPageId)

		// THEN
		assert.NoError(t, err)
		assert.Empty(t, result)
	})
}
