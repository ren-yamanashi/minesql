package dictionary

import (
	"path/filepath"
	"testing"

	"github.com/ren-yamanashi/minesql/internal/storage/btree"
	"github.com/ren-yamanashi/minesql/internal/storage/buffer"
	"github.com/ren-yamanashi/minesql/internal/storage/file"
	"github.com/ren-yamanashi/minesql/internal/storage/fsp"
	"github.com/ren-yamanashi/minesql/internal/storage/lock"
	"github.com/ren-yamanashi/minesql/internal/storage/page"
	"github.com/ren-yamanashi/minesql/internal/storage/redo"
	"github.com/stretchr/testify/assert"
)

func TestCreateUserMeta(t *testing.T) {
	t.Run("ユーザーメタデータを新規作成できる", func(t *testing.T) {
		// GIVEN
		bp := setupDictTestBufferPool(t)
		rl := setupDictTestRedoBuffer(t)

		// WHEN
		ctMtr := buffer.NewWriteMtr(bp, lock.SystemReservedTrxId, rl)
		um, err := CreateUserMeta(ctMtr)
		_ = ctMtr.Commit()

		// THEN
		assert.NoError(t, err)
		assert.False(t, um.tree.MetaPageId().IsInvalid())
	})
}

func TestUserMetaSearch(t *testing.T) {
	t.Run("SearchModeStart で全件スキャンできる", func(t *testing.T) {
		// GIVEN
		um, bp := setupTestUserMeta(t)
		mtr := buffer.NewMtr(bp)
		defer mtr.UnpinAll()
		_ = um.Insert(mtr, NewUserMetaRecord("alice", "localhost", []byte("auth1")))
		_ = um.Insert(mtr, NewUserMetaRecord("bob", "%", []byte("auth2")))

		// WHEN
		iter, err := um.Search(mtr, SearchModeStart{})
		assert.NoError(t, err)

		r1, ok1, err1 := iter.Next()
		r2, ok2, err2 := iter.Next()
		_, ok3, err3 := iter.Next()

		// THEN
		assert.NoError(t, err1)
		assert.True(t, ok1)
		assert.Equal(t, "alice", r1.Username())
		assert.Equal(t, "localhost", r1.Host())
		assert.Equal(t, []byte("auth1"), r1.AuthString())

		assert.NoError(t, err2)
		assert.True(t, ok2)
		assert.Equal(t, "bob", r2.Username())

		assert.NoError(t, err3)
		assert.False(t, ok3)
	})

	t.Run("SearchModeKey で指定したユーザーを検索できる", func(t *testing.T) {
		// GIVEN
		um, bp := setupTestUserMeta(t)
		mtr := buffer.NewMtr(bp)
		defer mtr.UnpinAll()
		_ = um.Insert(mtr, NewUserMetaRecord("alice", "localhost", []byte("auth1")))
		_ = um.Insert(mtr, NewUserMetaRecord("bob", "%", []byte("auth2")))

		// WHEN
		iter, err := um.Search(mtr, SearchModeKey{Key: [][]byte{[]byte("bob")}})
		assert.NoError(t, err)

		r, ok, err := iter.Next()

		// THEN
		assert.NoError(t, err)
		assert.True(t, ok)
		assert.Equal(t, "bob", r.Username())
		assert.Equal(t, "%", r.Host())
		assert.Equal(t, []byte("auth2"), r.AuthString())
	})

	t.Run("空のメタデータを検索するとレコードが返らない", func(t *testing.T) {
		// GIVEN
		um, bp := setupTestUserMeta(t)
		mtr := buffer.NewMtr(bp)
		defer mtr.UnpinAll()

		// WHEN
		iter, err := um.Search(mtr, SearchModeStart{})
		assert.NoError(t, err)

		_, ok, err := iter.Next()

		// THEN
		assert.NoError(t, err)
		assert.False(t, ok)
	})
}

func TestUserMetaInsert(t *testing.T) {
	t.Run("ユーザーを挿入できる", func(t *testing.T) {
		// GIVEN
		um, bp := setupTestUserMeta(t)
		mtr := buffer.NewMtr(bp)
		defer mtr.UnpinAll()

		// WHEN
		err := um.Insert(mtr, NewUserMetaRecord("alice", "localhost", []byte("auth123")))

		// THEN
		assert.NoError(t, err)
	})

	t.Run("同じユーザー名を重複挿入すると ErrDuplicateKey を返す", func(t *testing.T) {
		// GIVEN
		um, bp := setupTestUserMeta(t)
		mtr := buffer.NewMtr(bp)
		defer mtr.UnpinAll()
		_ = um.Insert(mtr, NewUserMetaRecord("alice", "localhost", []byte("auth1")))

		// WHEN
		err := um.Insert(mtr, NewUserMetaRecord("alice", "%", []byte("auth2")))

		// THEN
		assert.ErrorIs(t, err, btree.ErrDuplicateKey)
	})
}

// setupTestUserMeta はテスト用の UserMeta を作成する
func setupTestUserMeta(t *testing.T) (*UserMeta, *buffer.Pool) {
	t.Helper()
	bp := setupDictTestBufferPool(t)
	ctMtr := buffer.NewWriteMtr(bp, lock.SystemReservedTrxId, setupDictTestRedoBuffer(t))
	um, err := CreateUserMeta(ctMtr)
	_ = ctMtr.Commit()
	if err != nil {
		t.Fatalf("UserMeta の作成に失敗: %v", err)
	}
	return um, bp
}

// setupDictTestBufferPool は dictionary テスト用のバッファプールを作成する
//   - Create*Meta が要求する FSP ヘッダーを事前に初期化する (カタログ作成経路の InitHeader 相当)
func setupDictTestBufferPool(t *testing.T) *buffer.Pool {
	t.Helper()
	path := filepath.Join(t.TempDir(), "dict_test.db")
	fileId := page.FileId(0)
	hf, err := file.NewHeapFile(path)
	if err != nil {
		t.Fatalf("HeapFile の作成に失敗: %v", err)
	}
	t.Cleanup(func() { _ = hf.Close() })
	rl := setupDictTestRedoBuffer(t)
	bp := buffer.NewPool(page.Size*10, rl, nil)
	bp.RegisterHeapFile(fileId, hf)
	initMtr := buffer.NewWriteMtr(bp, lock.SystemReservedTrxId, rl)
	if err := fsp.InitHeader(initMtr, fileId); err != nil {
		t.Fatalf("fsp.InitHeader に失敗: %v", err)
	}
	if err := initMtr.Commit(); err != nil {
		t.Fatalf("InitHeader Mtr の Commit に失敗: %v", err)
	}
	return bp
}

// setupDictTestRedoBuffer は dictionary テスト用の Redo バッファを作成する
func setupDictTestRedoBuffer(t *testing.T) *redo.Buffer {
	t.Helper()
	rl, err := redo.NewBuffer(t.TempDir())
	if err != nil {
		t.Fatalf("redo.Buffer の作成に失敗: %v", err)
	}
	t.Cleanup(func() { _ = rl.Close() })
	return rl
}
