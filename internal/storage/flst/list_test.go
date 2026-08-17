package flst

import (
	"path/filepath"
	"testing"

	"github.com/ren-yamanashi/minesql/internal/storage/buffer"
	"github.com/ren-yamanashi/minesql/internal/storage/file"
	"github.com/ren-yamanashi/minesql/internal/storage/lock"
	"github.com/ren-yamanashi/minesql/internal/storage/page"
	"github.com/ren-yamanashi/minesql/internal/storage/redo"
	"github.com/stretchr/testify/assert"
)

const testFileId = page.FileId(1)

func TestInitBase(t *testing.T) {
	t.Run("空リストに初期化され First と Last は無効アドレスを返す", func(t *testing.T) {
		// GIVEN
		bp, redoLog := setupTest(t, 0)
		base := Address{PageNumber: 0, Offset: 0}

		// WHEN
		mtr := buffer.NewWriteMtr(bp, lock.TrxId(1), redoLog)
		if err := InitBase(mtr, testFileId, base); err != nil {
			t.Fatalf("InitBase に失敗: %v", err)
		}
		commitMtr(t, mtr)

		// THEN
		readMtr := buffer.NewMtr(bp)
		defer readMtr.UnpinAll()
		length, err := Length(readMtr, testFileId, base)
		assert.NoError(t, err)
		assert.Equal(t, uint32(0), length)
		first, err := First(readMtr, testFileId, base)
		assert.NoError(t, err)
		assert.True(t, first.IsInvalid())
		last, err := Last(readMtr, testFileId, base)
		assert.NoError(t, err)
		assert.True(t, last.IsInvalid())
	})
}

func TestAddFirst(t *testing.T) {
	t.Run("空リストへの追加で先頭と末尾が同じノードになる", func(t *testing.T) {
		// GIVEN
		bp, redoLog := setupTest(t, 0)
		base := Address{PageNumber: 0, Offset: 0}
		a := Address{PageNumber: 0, Offset: 100}
		setupList(t, bp, redoLog, base)

		// WHEN
		addFirst(t, bp, redoLog, base, a)

		// THEN
		assert.Equal(t, []Address{a}, collectForward(t, bp, base))
		assert.Equal(t, []Address{a}, collectBackward(t, bp, base))
		assert.Equal(t, uint32(1), listLength(t, bp, base))
	})

	t.Run("既存リストの先頭に追加される", func(t *testing.T) {
		// GIVEN
		bp, redoLog := setupTest(t, 0)
		base := Address{PageNumber: 0, Offset: 0}
		a := Address{PageNumber: 0, Offset: 100}
		b := Address{PageNumber: 0, Offset: 120}
		setupList(t, bp, redoLog, base, a)

		// WHEN
		addFirst(t, bp, redoLog, base, b)

		// THEN
		assert.Equal(t, []Address{b, a}, collectForward(t, bp, base))
		assert.Equal(t, []Address{a, b}, collectBackward(t, bp, base))
		assert.Equal(t, uint32(2), listLength(t, bp, base))
	})

	t.Run("base と node が同一アドレスの場合は panic する", func(t *testing.T) {
		// GIVEN
		bp, redoLog := setupTest(t, 0)
		base := Address{PageNumber: 0, Offset: 0}
		setupList(t, bp, redoLog, base)

		// THEN
		mtr := buffer.NewWriteMtr(bp, lock.TrxId(1), redoLog)
		defer mtr.UnpinAll()
		assert.Panics(t, func() {
			_ = AddFirst(mtr, testFileId, base, base)
		})
	})
}

func TestAddLast(t *testing.T) {
	t.Run("空リストへの追加で先頭と末尾が同じノードになる", func(t *testing.T) {
		// GIVEN
		bp, redoLog := setupTest(t, 0)
		base := Address{PageNumber: 0, Offset: 0}
		a := Address{PageNumber: 0, Offset: 100}
		setupList(t, bp, redoLog, base)

		// WHEN
		addLast(t, bp, redoLog, base, a)

		// THEN
		assert.Equal(t, []Address{a}, collectForward(t, bp, base))
		assert.Equal(t, []Address{a}, collectBackward(t, bp, base))
		assert.Equal(t, uint32(1), listLength(t, bp, base))
	})

	t.Run("既存リストの末尾に追加される", func(t *testing.T) {
		// GIVEN
		bp, redoLog := setupTest(t, 0)
		base := Address{PageNumber: 0, Offset: 0}
		a := Address{PageNumber: 0, Offset: 100}
		b := Address{PageNumber: 0, Offset: 120}
		setupList(t, bp, redoLog, base, a)

		// WHEN
		addLast(t, bp, redoLog, base, b)

		// THEN
		assert.Equal(t, []Address{a, b}, collectForward(t, bp, base))
		assert.Equal(t, []Address{b, a}, collectBackward(t, bp, base))
		assert.Equal(t, uint32(2), listLength(t, bp, base))
	})

	t.Run("AddFirst と混在させても順序が保たれる", func(t *testing.T) {
		// GIVEN
		bp, redoLog := setupTest(t, 0)
		base := Address{PageNumber: 0, Offset: 0}
		a := Address{PageNumber: 0, Offset: 100}
		b := Address{PageNumber: 0, Offset: 120}
		c := Address{PageNumber: 0, Offset: 140}
		setupList(t, bp, redoLog, base, a)

		// WHEN
		addFirst(t, bp, redoLog, base, b)
		addLast(t, bp, redoLog, base, c)

		// THEN
		assert.Equal(t, []Address{b, a, c}, collectForward(t, bp, base))
		assert.Equal(t, []Address{c, a, b}, collectBackward(t, bp, base))
		assert.Equal(t, uint32(3), listLength(t, bp, base))
	})

	t.Run("複数ページに跨るノードを連結できる", func(t *testing.T) {
		// GIVEN
		bp, redoLog := setupTest(t, 0, 1, 2)
		base := Address{PageNumber: 0, Offset: 0}
		a := Address{PageNumber: 1, Offset: 50}
		b := Address{PageNumber: 2, Offset: 80}
		c := Address{PageNumber: 1, Offset: 200}

		// WHEN
		setupList(t, bp, redoLog, base, a, b, c)

		// THEN
		assert.Equal(t, []Address{a, b, c}, collectForward(t, bp, base))
		assert.Equal(t, []Address{c, b, a}, collectBackward(t, bp, base))
		assert.Equal(t, uint32(3), listLength(t, bp, base))
	})

	t.Run("base と node が同一アドレスの場合は panic する", func(t *testing.T) {
		// GIVEN
		bp, redoLog := setupTest(t, 0)
		base := Address{PageNumber: 0, Offset: 0}
		setupList(t, bp, redoLog, base)

		// THEN
		mtr := buffer.NewWriteMtr(bp, lock.TrxId(1), redoLog)
		defer mtr.UnpinAll()
		assert.Panics(t, func() {
			_ = AddLast(mtr, testFileId, base, base)
		})
	})
}

func TestRemove(t *testing.T) {
	t.Run("先頭ノードを除去できる", func(t *testing.T) {
		// GIVEN
		bp, redoLog := setupTest(t, 0)
		base := Address{PageNumber: 0, Offset: 0}
		a := Address{PageNumber: 0, Offset: 100}
		b := Address{PageNumber: 0, Offset: 120}
		c := Address{PageNumber: 0, Offset: 140}
		setupList(t, bp, redoLog, base, a, b, c)

		// WHEN
		removeNode(t, bp, redoLog, base, a)

		// THEN
		assert.Equal(t, []Address{b, c}, collectForward(t, bp, base))
		assert.Equal(t, []Address{c, b}, collectBackward(t, bp, base))
		assert.Equal(t, uint32(2), listLength(t, bp, base))
	})

	t.Run("中間ノードを除去できる", func(t *testing.T) {
		// GIVEN
		bp, redoLog := setupTest(t, 0)
		base := Address{PageNumber: 0, Offset: 0}
		a := Address{PageNumber: 0, Offset: 100}
		b := Address{PageNumber: 0, Offset: 120}
		c := Address{PageNumber: 0, Offset: 140}
		setupList(t, bp, redoLog, base, a, b, c)

		// WHEN
		removeNode(t, bp, redoLog, base, b)

		// THEN
		assert.Equal(t, []Address{a, c}, collectForward(t, bp, base))
		assert.Equal(t, []Address{c, a}, collectBackward(t, bp, base))
		assert.Equal(t, uint32(2), listLength(t, bp, base))
	})

	t.Run("末尾ノードを除去できる", func(t *testing.T) {
		// GIVEN
		bp, redoLog := setupTest(t, 0)
		base := Address{PageNumber: 0, Offset: 0}
		a := Address{PageNumber: 0, Offset: 100}
		b := Address{PageNumber: 0, Offset: 120}
		c := Address{PageNumber: 0, Offset: 140}
		setupList(t, bp, redoLog, base, a, b, c)

		// WHEN
		removeNode(t, bp, redoLog, base, c)

		// THEN
		assert.Equal(t, []Address{a, b}, collectForward(t, bp, base))
		assert.Equal(t, []Address{b, a}, collectBackward(t, bp, base))
		assert.Equal(t, uint32(2), listLength(t, bp, base))
	})

	t.Run("唯一のノードを除去すると空リストに戻る", func(t *testing.T) {
		// GIVEN
		bp, redoLog := setupTest(t, 0)
		base := Address{PageNumber: 0, Offset: 0}
		a := Address{PageNumber: 0, Offset: 100}
		setupList(t, bp, redoLog, base, a)

		// WHEN
		removeNode(t, bp, redoLog, base, a)

		// THEN
		assert.Empty(t, collectForward(t, bp, base))
		assert.Empty(t, collectBackward(t, bp, base))
		assert.Equal(t, uint32(0), listLength(t, bp, base))
	})

	t.Run("空リストに対する Remove は panic する", func(t *testing.T) {
		// GIVEN
		bp, redoLog := setupTest(t, 0)
		base := Address{PageNumber: 0, Offset: 0}
		node := Address{PageNumber: 0, Offset: 100}
		setupList(t, bp, redoLog, base)

		// THEN
		mtr := buffer.NewWriteMtr(bp, lock.TrxId(1), redoLog)
		defer mtr.UnpinAll()
		assert.Panics(t, func() {
			_ = Remove(mtr, testFileId, base, node)
		})
	})
}

func TestLength(t *testing.T) {
	t.Run("リストの長さを取得できる", func(t *testing.T) {
		// GIVEN
		bp, redoLog := setupTest(t, 0)
		base := Address{PageNumber: 0, Offset: 0}
		a := Address{PageNumber: 0, Offset: 100}
		b := Address{PageNumber: 0, Offset: 120}
		c := Address{PageNumber: 0, Offset: 140}
		setupList(t, bp, redoLog, base, a, b, c)

		// WHEN
		mtr := buffer.NewMtr(bp)
		defer mtr.UnpinAll()
		length, err := Length(mtr, testFileId, base)

		// THEN
		assert.NoError(t, err)
		assert.Equal(t, uint32(3), length)
	})
}

func TestFirst(t *testing.T) {
	t.Run("リストの先頭ノードのアドレスを返す", func(t *testing.T) {
		// GIVEN
		bp, redoLog := setupTest(t, 0)
		base := Address{PageNumber: 0, Offset: 0}
		a := Address{PageNumber: 0, Offset: 100}
		b := Address{PageNumber: 0, Offset: 120}
		setupList(t, bp, redoLog, base, a, b)

		// WHEN
		mtr := buffer.NewMtr(bp)
		defer mtr.UnpinAll()
		got, err := First(mtr, testFileId, base)

		// THEN
		assert.NoError(t, err)
		assert.Equal(t, a, got)
	})

	t.Run("空リストは無効アドレスを返す", func(t *testing.T) {
		// GIVEN
		bp, redoLog := setupTest(t, 0)
		base := Address{PageNumber: 0, Offset: 0}
		setupList(t, bp, redoLog, base)

		// WHEN
		mtr := buffer.NewMtr(bp)
		defer mtr.UnpinAll()
		got, err := First(mtr, testFileId, base)

		// THEN
		assert.NoError(t, err)
		assert.True(t, got.IsInvalid())
	})
}

func TestLast(t *testing.T) {
	t.Run("リストの末尾ノードのアドレスを返す", func(t *testing.T) {
		// GIVEN
		bp, redoLog := setupTest(t, 0)
		base := Address{PageNumber: 0, Offset: 0}
		a := Address{PageNumber: 0, Offset: 100}
		b := Address{PageNumber: 0, Offset: 120}
		setupList(t, bp, redoLog, base, a, b)

		// WHEN
		mtr := buffer.NewMtr(bp)
		defer mtr.UnpinAll()
		got, err := Last(mtr, testFileId, base)

		// THEN
		assert.NoError(t, err)
		assert.Equal(t, b, got)
	})

	t.Run("空リストは無効アドレスを返す", func(t *testing.T) {
		// GIVEN
		bp, redoLog := setupTest(t, 0)
		base := Address{PageNumber: 0, Offset: 0}
		setupList(t, bp, redoLog, base)

		// WHEN
		mtr := buffer.NewMtr(bp)
		defer mtr.UnpinAll()
		got, err := Last(mtr, testFileId, base)

		// THEN
		assert.NoError(t, err)
		assert.True(t, got.IsInvalid())
	})
}

func TestNext(t *testing.T) {
	t.Run("ノードの次のアドレスを返す", func(t *testing.T) {
		// GIVEN
		bp, redoLog := setupTest(t, 0)
		base := Address{PageNumber: 0, Offset: 0}
		a := Address{PageNumber: 0, Offset: 100}
		b := Address{PageNumber: 0, Offset: 120}
		setupList(t, bp, redoLog, base, a, b)

		// WHEN
		mtr := buffer.NewMtr(bp)
		defer mtr.UnpinAll()
		got, err := Next(mtr, testFileId, a)

		// THEN
		assert.NoError(t, err)
		assert.Equal(t, b, got)
	})

	t.Run("末尾ノードの Next は無効アドレスを返す", func(t *testing.T) {
		// GIVEN
		bp, redoLog := setupTest(t, 0)
		base := Address{PageNumber: 0, Offset: 0}
		a := Address{PageNumber: 0, Offset: 100}
		b := Address{PageNumber: 0, Offset: 120}
		setupList(t, bp, redoLog, base, a, b)

		// WHEN
		mtr := buffer.NewMtr(bp)
		defer mtr.UnpinAll()
		got, err := Next(mtr, testFileId, b)

		// THEN
		assert.NoError(t, err)
		assert.True(t, got.IsInvalid())
	})
}

func TestPrev(t *testing.T) {
	t.Run("ノードの前のアドレスを返す", func(t *testing.T) {
		// GIVEN
		bp, redoLog := setupTest(t, 0)
		base := Address{PageNumber: 0, Offset: 0}
		a := Address{PageNumber: 0, Offset: 100}
		b := Address{PageNumber: 0, Offset: 120}
		setupList(t, bp, redoLog, base, a, b)

		// WHEN
		mtr := buffer.NewMtr(bp)
		defer mtr.UnpinAll()
		got, err := Prev(mtr, testFileId, b)

		// THEN
		assert.NoError(t, err)
		assert.Equal(t, a, got)
	})

	t.Run("先頭ノードの Prev は無効アドレスを返す", func(t *testing.T) {
		// GIVEN
		bp, redoLog := setupTest(t, 0)
		base := Address{PageNumber: 0, Offset: 0}
		a := Address{PageNumber: 0, Offset: 100}
		b := Address{PageNumber: 0, Offset: 120}
		setupList(t, bp, redoLog, base, a, b)

		// WHEN
		mtr := buffer.NewMtr(bp)
		defer mtr.UnpinAll()
		got, err := Prev(mtr, testFileId, a)

		// THEN
		assert.NoError(t, err)
		assert.True(t, got.IsInvalid())
	})
}

// setupTest はテスト用の BufferPool と Redo バッファを作成し、指定した PageNumber のゼロ埋めページを用意する
func setupTest(t *testing.T, pageNumbers ...page.PageNumber) (*buffer.Pool, *redo.Buffer) {
	t.Helper()
	redoLog, err := redo.NewBuffer(t.TempDir())
	if err != nil {
		t.Fatalf("redo.Buffer の作成に失敗: %v", err)
	}
	t.Cleanup(func() { _ = redoLog.Close() })
	path := filepath.Join(t.TempDir(), "test.db")
	hf, err := file.NewHeapFile(path)
	if err != nil {
		t.Fatalf("HeapFile の作成に失敗: %v", err)
	}
	t.Cleanup(func() { _ = hf.Close() })
	bp := buffer.NewPool(page.Size*20, redoLog, nil)
	bp.RegisterHeapFile(testFileId, hf)
	for _, pn := range pageNumbers {
		if _, err := bp.AddPage(page.NewId(testFileId, pn)); err != nil {
			t.Fatalf("AddPage に失敗: %v", err)
		}
	}
	return bp, redoLog
}

// setupList は base を初期化し nodes を先頭から順に AddLast で連結する
func setupList(t *testing.T, bp *buffer.Pool, redoLog *redo.Buffer, base Address, nodes ...Address) {
	t.Helper()
	mtr := buffer.NewWriteMtr(bp, lock.TrxId(1), redoLog)
	if err := InitBase(mtr, testFileId, base); err != nil {
		t.Fatalf("InitBase に失敗: %v", err)
	}
	for _, node := range nodes {
		if err := AddLast(mtr, testFileId, base, node); err != nil {
			t.Fatalf("AddLast に失敗: %v", err)
		}
	}
	commitMtr(t, mtr)
}

func addFirst(t *testing.T, bp *buffer.Pool, redoLog *redo.Buffer, base, node Address) {
	t.Helper()
	mtr := buffer.NewWriteMtr(bp, lock.TrxId(1), redoLog)
	if err := AddFirst(mtr, testFileId, base, node); err != nil {
		t.Fatalf("AddFirst に失敗: %v", err)
	}
	commitMtr(t, mtr)
}

func addLast(t *testing.T, bp *buffer.Pool, redoLog *redo.Buffer, base, node Address) {
	t.Helper()
	mtr := buffer.NewWriteMtr(bp, lock.TrxId(1), redoLog)
	if err := AddLast(mtr, testFileId, base, node); err != nil {
		t.Fatalf("AddLast に失敗: %v", err)
	}
	commitMtr(t, mtr)
}

func removeNode(t *testing.T, bp *buffer.Pool, redoLog *redo.Buffer, base, node Address) {
	t.Helper()
	mtr := buffer.NewWriteMtr(bp, lock.TrxId(1), redoLog)
	if err := Remove(mtr, testFileId, base, node); err != nil {
		t.Fatalf("Remove に失敗: %v", err)
	}
	commitMtr(t, mtr)
}

// collectForward は First と Next でリストを先頭から辿ったアドレス列を返す
func collectForward(t *testing.T, bp *buffer.Pool, base Address) []Address {
	t.Helper()
	mtr := buffer.NewMtr(bp)
	defer mtr.UnpinAll()
	var nodes []Address
	addr, err := First(mtr, testFileId, base)
	if err != nil {
		t.Fatalf("First に失敗: %v", err)
	}
	for !addr.IsInvalid() {
		nodes = append(nodes, addr)
		addr, err = Next(mtr, testFileId, addr)
		if err != nil {
			t.Fatalf("Next に失敗: %v", err)
		}
	}
	return nodes
}

// collectBackward は Last と Prev でリストを末尾から辿ったアドレス列を返す
func collectBackward(t *testing.T, bp *buffer.Pool, base Address) []Address {
	t.Helper()
	mtr := buffer.NewMtr(bp)
	defer mtr.UnpinAll()
	var nodes []Address
	addr, err := Last(mtr, testFileId, base)
	if err != nil {
		t.Fatalf("Last に失敗: %v", err)
	}
	for !addr.IsInvalid() {
		nodes = append(nodes, addr)
		addr, err = Prev(mtr, testFileId, addr)
		if err != nil {
			t.Fatalf("Prev に失敗: %v", err)
		}
	}
	return nodes
}

func listLength(t *testing.T, bp *buffer.Pool, base Address) uint32 {
	t.Helper()
	mtr := buffer.NewMtr(bp)
	defer mtr.UnpinAll()
	length, err := Length(mtr, testFileId, base)
	if err != nil {
		t.Fatalf("Length に失敗: %v", err)
	}
	return length
}

func commitMtr(t *testing.T, mtr *buffer.Mtr) {
	t.Helper()
	if err := mtr.Commit(); err != nil {
		t.Fatalf("mtr.Commit に失敗: %v", err)
	}
}
