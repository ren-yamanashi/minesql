package access

import (
	"path/filepath"
	"testing"

	"github.com/ren-yamanashi/minesql/internal/storage/btree"
	"github.com/ren-yamanashi/minesql/internal/storage/buffer"
	"github.com/ren-yamanashi/minesql/internal/storage/file"
	"github.com/ren-yamanashi/minesql/internal/storage/page"
	"github.com/stretchr/testify/assert"
)

func TestSecondaryIteratorNext(t *testing.T) {
	t.Run("セカンダリインデックスからプライマリインデックスを辿ってレコードを返す", func(t *testing.T) {
		// GIVEN
		bp := setupSecondaryTestBufferPool(t)
		primaryBt := setupBtreeWithBp(t, bp)
		secondaryBt := setupBtreeWithBp(t, bp)

		// プライマリに pk=key1, nonKey=val1 を挿入
		insertPrimaryRecord(primaryBt, []byte{0x00}, [][]byte{[]byte("key1")}, [][]byte{[]byte("val1")})

		// セカンダリに sk=name1, pk=key1 を挿入 (skCount=1)
		sr := newSecondaryRecord(1, 0x00, [][]byte{[]byte("name1"), []byte("key1")})
		if err := secondaryBt.Insert(sr.encode()); err != nil {
			t.Fatal(err)
		}

		iter, err := secondaryBt.Search(btree.SearchModeStart{})
		assert.NoError(t, err)
		si := newSecondaryIterator(iter, primaryBt, 1, 1)

		// WHEN
		result, ok, err := si.Next()

		// THEN
		assert.NoError(t, err)
		assert.True(t, ok)
		assert.Equal(t, [][]byte{[]byte("name1")}, result.SecondaryKey)
		assert.Equal(t, [][]byte{[]byte("key1"), []byte("val1")}, result.Record)
	})

	t.Run("削除マーク付きセカンダリレコードをスキップする", func(t *testing.T) {
		// GIVEN
		bp := setupSecondaryTestBufferPool(t)
		primaryBt := setupBtreeWithBp(t, bp)
		secondaryBt := setupBtreeWithBp(t, bp)

		insertPrimaryRecord(primaryBt, []byte{0x00}, [][]byte{[]byte("key1")}, [][]byte{[]byte("val1")})
		insertPrimaryRecord(primaryBt, []byte{0x00}, [][]byte{[]byte("key2")}, [][]byte{[]byte("val2")})

		// 削除済みのセカンダリレコード
		deleted := newSecondaryRecord(1, 0x01, [][]byte{[]byte("aaa"), []byte("key1")})
		if err := secondaryBt.Insert(deleted.encode()); err != nil {
			t.Fatal(err)
		}
		// 可視のセカンダリレコード
		visible := newSecondaryRecord(1, 0x00, [][]byte{[]byte("bbb"), []byte("key2")})
		if err := secondaryBt.Insert(visible.encode()); err != nil {
			t.Fatal(err)
		}

		iter, err := secondaryBt.Search(btree.SearchModeStart{})
		assert.NoError(t, err)
		si := newSecondaryIterator(iter, primaryBt, 1, 1)

		// WHEN
		result, ok, err := si.Next()

		// THEN
		assert.NoError(t, err)
		assert.True(t, ok)
		assert.Equal(t, [][]byte{[]byte("bbb")}, result.SecondaryKey)
		assert.Equal(t, [][]byte{[]byte("key2"), []byte("val2")}, result.Record)
	})

	t.Run("セカンダリインデックスが空の場合は false を返す", func(t *testing.T) {
		// GIVEN
		bp := setupSecondaryTestBufferPool(t)
		primaryBt := setupBtreeWithBp(t, bp)
		secondaryBt := setupBtreeWithBp(t, bp)

		iter, err := secondaryBt.Search(btree.SearchModeStart{})
		assert.NoError(t, err)
		si := newSecondaryIterator(iter, primaryBt, 1, 1)

		// WHEN
		result, ok, err := si.Next()

		// THEN
		assert.NoError(t, err)
		assert.False(t, ok)
		assert.Nil(t, result)
	})
}

func TestSecondaryIteratorNextIndexOnly(t *testing.T) {
	t.Run("セカンダリインデックスのみからセカンダリキーとプライマリキーを返す", func(t *testing.T) {
		// GIVEN
		bp := setupSecondaryTestBufferPool(t)
		primaryBt := setupBtreeWithBp(t, bp)
		secondaryBt := setupBtreeWithBp(t, bp)

		sr := newSecondaryRecord(1, 0x00, [][]byte{[]byte("name1"), []byte("key1")})
		if err := secondaryBt.Insert(sr.encode()); err != nil {
			t.Fatal(err)
		}

		iter, err := secondaryBt.Search(btree.SearchModeStart{})
		assert.NoError(t, err)
		si := newSecondaryIterator(iter, primaryBt, 1, 1)

		// WHEN
		result, ok, err := si.NextIndexOnly()

		// THEN
		assert.NoError(t, err)
		assert.True(t, ok)
		assert.Equal(t, [][]byte{[]byte("name1")}, result.SecondaryKey)
		assert.Equal(t, [][]byte{[]byte("key1")}, result.PKValues)
		assert.Nil(t, result.Record)
	})

	t.Run("削除マーク付きレコードをスキップする", func(t *testing.T) {
		// GIVEN
		bp := setupSecondaryTestBufferPool(t)
		primaryBt := setupBtreeWithBp(t, bp)
		secondaryBt := setupBtreeWithBp(t, bp)

		deleted := newSecondaryRecord(1, 0x01, [][]byte{[]byte("aaa"), []byte("key1")})
		if err := secondaryBt.Insert(deleted.encode()); err != nil {
			t.Fatal(err)
		}
		visible := newSecondaryRecord(1, 0x00, [][]byte{[]byte("bbb"), []byte("key2")})
		if err := secondaryBt.Insert(visible.encode()); err != nil {
			t.Fatal(err)
		}

		iter, err := secondaryBt.Search(btree.SearchModeStart{})
		assert.NoError(t, err)
		si := newSecondaryIterator(iter, primaryBt, 1, 1)

		// WHEN
		result, ok, err := si.NextIndexOnly()

		// THEN
		assert.NoError(t, err)
		assert.True(t, ok)
		assert.Equal(t, [][]byte{[]byte("bbb")}, result.SecondaryKey)
		assert.Equal(t, [][]byte{[]byte("key2")}, result.PKValues)
	})

	t.Run("セカンダリインデックスが空の場合は false を返す", func(t *testing.T) {
		// GIVEN
		bp := setupSecondaryTestBufferPool(t)
		primaryBt := setupBtreeWithBp(t, bp)
		secondaryBt := setupBtreeWithBp(t, bp)

		iter, err := secondaryBt.Search(btree.SearchModeStart{})
		assert.NoError(t, err)
		si := newSecondaryIterator(iter, primaryBt, 1, 1)

		// WHEN
		result, ok, err := si.NextIndexOnly()

		// THEN
		assert.NoError(t, err)
		assert.False(t, ok)
		assert.Nil(t, result)
	})

	t.Run("全レコードが削除済みの場合は false を返す", func(t *testing.T) {
		// GIVEN
		bp := setupSecondaryTestBufferPool(t)
		primaryBt := setupBtreeWithBp(t, bp)
		secondaryBt := setupBtreeWithBp(t, bp)

		d1 := newSecondaryRecord(1, 0x01, [][]byte{[]byte("aaa"), []byte("key1")})
		if err := secondaryBt.Insert(d1.encode()); err != nil {
			t.Fatal(err)
		}
		d2 := newSecondaryRecord(1, 0x01, [][]byte{[]byte("bbb"), []byte("key2")})
		if err := secondaryBt.Insert(d2.encode()); err != nil {
			t.Fatal(err)
		}

		iter, err := secondaryBt.Search(btree.SearchModeStart{})
		assert.NoError(t, err)
		si := newSecondaryIterator(iter, primaryBt, 1, 1)

		// WHEN
		result, ok, err := si.NextIndexOnly()

		// THEN
		assert.NoError(t, err)
		assert.False(t, ok)
		assert.Nil(t, result)
	})
}

// setupSecondaryTestBufferPool はセカンダリテスト用のバッファプールを作成する (複数 B+Tree を同じファイルで共有)
func setupSecondaryTestBufferPool(t *testing.T) *buffer.BufferPool {
	t.Helper()
	tmpdir := t.TempDir()
	path := filepath.Join(tmpdir, "secondary_test.db")
	fileId := page.FileId(0)
	heapFile, err := file.NewHeapFile(fileId, path)
	if err != nil {
		t.Fatalf("HeapFile の作成に失敗: %v", err)
	}
	t.Cleanup(func() { _ = heapFile.Close() })
	bp := buffer.NewBufferPool(page.PageSize * 20)
	bp.RegisterHeapFile(fileId, heapFile)
	return bp
}

// setupBtreeWithBp は既存のバッファプール上に B+Tree を作成する
func setupBtreeWithBp(t *testing.T, bp *buffer.BufferPool) *btree.Btree {
	t.Helper()
	bt, err := btree.CreateBtree(bp, page.FileId(0))
	if err != nil {
		t.Fatalf("B+Tree の作成に失敗: %v", err)
	}
	return bt
}
