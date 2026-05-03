package access

import (
	"path/filepath"
	"testing"

	"github.com/ren-yamanashi/minesql/internal/storage/btree"
	"github.com/ren-yamanashi/minesql/internal/storage/btree/node"
	"github.com/ren-yamanashi/minesql/internal/storage/buffer"
	"github.com/ren-yamanashi/minesql/internal/storage/encode"
	"github.com/ren-yamanashi/minesql/internal/storage/file"
	"github.com/ren-yamanashi/minesql/internal/storage/page"
	"github.com/stretchr/testify/assert"
)

func TestPrimaryIteratorNext(t *testing.T) {
	t.Run("レコードをデコードして返す", func(t *testing.T) {
		// GIVEN
		bt := setupPrimaryBtree(t)
		insertPrimaryRecord(bt, []byte{0x00}, [][]byte{[]byte("key1")}, [][]byte{[]byte("val1")})

		iter, err := bt.Search(btree.SearchModeStart{})
		assert.NoError(t, err)
		pi := newPrimaryIterator(iter)

		// WHEN
		result, ok, err := pi.Next()

		// THEN
		assert.NoError(t, err)
		assert.True(t, ok)
		assert.Equal(t, [][]byte{[]byte("key1"), []byte("val1")}, result)
	})

	t.Run("複数レコードを順に取得できる", func(t *testing.T) {
		// GIVEN
		bt := setupPrimaryBtree(t)
		insertPrimaryRecord(bt, []byte{0x00}, [][]byte{[]byte("a")}, [][]byte{[]byte("v1")})
		insertPrimaryRecord(bt, []byte{0x00}, [][]byte{[]byte("b")}, [][]byte{[]byte("v2")})

		iter, err := bt.Search(btree.SearchModeStart{})
		assert.NoError(t, err)
		pi := newPrimaryIterator(iter)

		// WHEN
		r1, ok1, err1 := pi.Next()
		r2, ok2, err2 := pi.Next()
		_, ok3, err3 := pi.Next()

		// THEN
		assert.NoError(t, err1)
		assert.True(t, ok1)
		assert.Equal(t, [][]byte{[]byte("a"), []byte("v1")}, r1)

		assert.NoError(t, err2)
		assert.True(t, ok2)
		assert.Equal(t, [][]byte{[]byte("b"), []byte("v2")}, r2)

		assert.NoError(t, err3)
		assert.False(t, ok3)
	})

	t.Run("削除マーク付きレコードをスキップする", func(t *testing.T) {
		// GIVEN
		bt := setupPrimaryBtree(t)
		insertPrimaryRecord(bt, []byte{0x01}, [][]byte{[]byte("deleted")}, [][]byte{[]byte("x")})
		insertPrimaryRecord(bt, []byte{0x00}, [][]byte{[]byte("visible")}, [][]byte{[]byte("y")})

		iter, err := bt.Search(btree.SearchModeStart{})
		assert.NoError(t, err)
		pi := newPrimaryIterator(iter)

		// WHEN
		result, ok, err := pi.Next()

		// THEN
		assert.NoError(t, err)
		assert.True(t, ok)
		assert.Equal(t, [][]byte{[]byte("visible"), []byte("y")}, result)
	})

	t.Run("全レコードが削除済みの場合は false を返す", func(t *testing.T) {
		// GIVEN
		bt := setupPrimaryBtree(t)
		insertPrimaryRecord(bt, []byte{0x01}, [][]byte{[]byte("d1")}, [][]byte{[]byte("x")})
		insertPrimaryRecord(bt, []byte{0x01}, [][]byte{[]byte("d2")}, [][]byte{[]byte("y")})

		iter, err := bt.Search(btree.SearchModeStart{})
		assert.NoError(t, err)
		pi := newPrimaryIterator(iter)

		// WHEN
		result, ok, err := pi.Next()

		// THEN
		assert.NoError(t, err)
		assert.False(t, ok)
		assert.Nil(t, result)
	})

	t.Run("レコードが空の場合は false を返す", func(t *testing.T) {
		// GIVEN
		bt := setupPrimaryBtree(t)

		iter, err := bt.Search(btree.SearchModeStart{})
		assert.NoError(t, err)
		pi := newPrimaryIterator(iter)

		// WHEN
		result, ok, err := pi.Next()

		// THEN
		assert.NoError(t, err)
		assert.False(t, ok)
		assert.Nil(t, result)
	})
}

// setupPrimaryBtree はテスト用の B+Tree をセットアップする
func setupPrimaryBtree(t *testing.T) *btree.Btree {
	t.Helper()
	tmpdir := t.TempDir()
	path := filepath.Join(tmpdir, "primary_test.db")
	fileId := page.FileId(0)
	heapFile, err := file.NewHeapFile(fileId, path)
	if err != nil {
		t.Fatalf("HeapFile の作成に失敗: %v", err)
	}
	bp := buffer.NewBufferPool(page.PageSize * 10)
	bp.RegisterHeapFile(fileId, heapFile)

	bt, err := btree.CreateBtree(bp, fileId)
	if err != nil {
		t.Fatalf("B+Tree の作成に失敗: %v", err)
	}
	return bt
}

// insertPrimaryRecord はエンコード済みのレコードを B+Tree に挿入する
func insertPrimaryRecord(bt *btree.Btree, header []byte, keyParts, nonKeyParts [][]byte) {
	var encodedKey []byte
	encode.Encode(keyParts, &encodedKey)
	var encodedNonKey []byte
	encode.Encode(nonKeyParts, &encodedNonKey)

	record := node.NewRecord(header, encodedKey, encodedNonKey)
	if err := bt.Insert(record); err != nil {
		panic(err)
	}
}
