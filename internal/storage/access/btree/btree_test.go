package btree

import (
	"fmt"
	"minesql/internal/storage/access/btree/node"
	"minesql/internal/storage/bufferpool"
	"minesql/internal/storage/disk"
	"minesql/internal/storage/page"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestCreateBTree(t *testing.T) {
	t.Run("B+Tree が作成され、空の状態で検索できる", func(t *testing.T) {
		// GIVEN & WHEN
		bt, bpm := setupBTree(t)

		// THEN
		pairs := bt.collectAllPairs(bpm)
		assert.Equal(t, 0, len(pairs))
	})
}

func TestInsert(t *testing.T) {
	t.Run("1 つのペアを挿入して検索できる", func(t *testing.T) {
		// GIVEN
		bt, bpm := setupBTree(t)

		// WHEN
		err := bt.Insert(bpm, node.NewPair([]byte("key1"), []byte("val1")))

		// THEN
		assert.NoError(t, err)
		pairs := bt.collectAllPairs(bpm)
		assert.Equal(t, 1, len(pairs))
		assert.Equal(t, "key1", string(pairs[0].Key))
		assert.Equal(t, "val1", string(pairs[0].Value))
	})

	t.Run("重複キーを挿入するとエラーが返る", func(t *testing.T) {
		// GIVEN
		bt, bpm := setupBTree(t)
		bt.mustInsert(bpm, "key1", "val1")

		// WHEN
		err := bt.Insert(bpm, node.NewPair([]byte("key1"), []byte("val2")))

		// THEN
		assert.ErrorIs(t, err, ErrDuplicateKey)
	})

	t.Run("挿入順に関わらずキーが昇順でソートされる", func(t *testing.T) {
		// GIVEN
		bt, bpm := setupBTree(t)

		// WHEN: 降順に挿入
		bt.mustInsert(bpm, "ccc", "v3")
		bt.mustInsert(bpm, "aaa", "v1")
		bt.mustInsert(bpm, "bbb", "v2")

		// THEN: 昇順で取得できる
		pairs := bt.collectAllPairs(bpm)
		assert.Equal(t, 3, len(pairs))
		assert.Equal(t, "aaa", string(pairs[0].Key))
		assert.Equal(t, "bbb", string(pairs[1].Key))
		assert.Equal(t, "ccc", string(pairs[2].Key))
	})

	t.Run("降順に多数のペアを挿入してもすべて取得できる", func(t *testing.T) {
		// GIVEN
		bt, bpm := setupBTree(t)

		// WHEN: 降順に挿入
		numPairs := 100
		for i := numPairs - 1; i >= 0; i-- {
			key := fmt.Sprintf("key%03d", i)
			val := fmt.Sprintf("val%03d", i)
			bt.mustInsert(bpm, key, val)
		}

		// THEN: 全ペアが昇順で取得できる
		pairs := bt.collectAllPairs(bpm)
		assert.Equal(t, numPairs, len(pairs))
		for i, pair := range pairs {
			expectedKey := fmt.Sprintf("key%03d", i)
			expectedVal := fmt.Sprintf("val%03d", i)
			assert.Equal(t, expectedKey, string(pair.Key))
			assert.Equal(t, expectedVal, string(pair.Value))
		}
	})

	t.Run("分割後に重複キーを挿入するとエラーが返る", func(t *testing.T) {
		// GIVEN: 多数のペアを挿入してノード分割を発生させる
		bt, bpm := setupBTree(t)
		numPairs := 100
		for i := range numPairs {
			key := fmt.Sprintf("key%03d", i)
			val := fmt.Sprintf("val%03d", i)
			bt.mustInsert(bpm, key, val)
		}

		// WHEN: 既存のキーで挿入を試みる
		err := bt.Insert(bpm, node.NewPair([]byte("key050"), []byte("dup")))

		// THEN
		assert.ErrorIs(t, err, ErrDuplicateKey)
	})

	t.Run("多数のペアを挿入してルート分割が発生しても全ペアが取得できる", func(t *testing.T) {
		// GIVEN
		bt, bpm := setupBTree(t)

		// WHEN: 多数のペアを挿入してノード分割を発生させる
		numPairs := 100
		for i := range numPairs {
			key := fmt.Sprintf("key%03d", i)
			val := fmt.Sprintf("val%03d", i)
			bt.mustInsert(bpm, key, val)
		}

		// THEN: 全ペアが昇順で取得できる
		pairs := bt.collectAllPairs(bpm)
		assert.Equal(t, numPairs, len(pairs))
		for i, pair := range pairs {
			expectedKey := fmt.Sprintf("key%03d", i)
			expectedVal := fmt.Sprintf("val%03d", i)
			assert.Equal(t, expectedKey, string(pair.Key))
			assert.Equal(t, expectedVal, string(pair.Value))
		}
	})
}

func TestSearch(t *testing.T) {
	t.Run("SearchModeStart で空のツリーを検索すると結果が空になる", func(t *testing.T) {
		// GIVEN
		bt, bpm := setupBTree(t)

		// WHEN
		iter, err := bt.Search(bpm, SearchModeStart{})

		// THEN
		assert.NoError(t, err)
		_, ok := iter.Get()
		assert.False(t, ok)
	})

	t.Run("SearchModeStart で先頭のペアが取得できる", func(t *testing.T) {
		// GIVEN
		bt, bpm := setupBTree(t)
		bt.mustInsert(bpm, "aaa", "v1")
		bt.mustInsert(bpm, "bbb", "v2")
		bt.mustInsert(bpm, "ccc", "v3")

		// WHEN
		iter, err := bt.Search(bpm, SearchModeStart{})

		// THEN
		assert.NoError(t, err)
		pair, ok := iter.Get()
		assert.True(t, ok)
		assert.Equal(t, "aaa", string(pair.Key))
	})

	t.Run("SearchModeStart で分割が発生した B+Tree の全ペアを走査できる", func(t *testing.T) {
		// GIVEN: 多数のペアを挿入してノード分割を発生させる
		bt, bpm := setupBTree(t)
		numPairs := 100
		for i := range numPairs {
			key := fmt.Sprintf("key%03d", i)
			val := fmt.Sprintf("val%03d", i)
			bt.mustInsert(bpm, key, val)
		}

		// WHEN: SearchModeStart で Next() を繰り返し呼ぶ
		iter, err := bt.Search(bpm, SearchModeStart{})
		assert.NoError(t, err)
		var pairs []node.Pair
		for {
			pair, ok, err := iter.Next(bpm)
			assert.NoError(t, err)
			if !ok {
				break
			}
			keyCopy := make([]byte, len(pair.Key))
			copy(keyCopy, pair.Key)
			pairs = append(pairs, node.NewPair(keyCopy, nil))
		}

		// THEN: 全ペアが昇順で取得でき、最後に ok=false が返る
		assert.Equal(t, numPairs, len(pairs))
		for i, pair := range pairs {
			expectedKey := fmt.Sprintf("key%03d", i)
			assert.Equal(t, expectedKey, string(pair.Key))
		}
	})

	t.Run("SearchModeKey で指定したキーの位置からイテレータが開始される", func(t *testing.T) {
		// GIVEN
		bt, bpm := setupBTree(t)
		bt.mustInsert(bpm, "aaa", "v1")
		bt.mustInsert(bpm, "bbb", "v2")
		bt.mustInsert(bpm, "ccc", "v3")

		// WHEN
		iter, err := bt.Search(bpm, SearchModeKey{Key: []byte("bbb")})

		// THEN
		assert.NoError(t, err)
		pair, ok := iter.Get()
		assert.True(t, ok)
		assert.Equal(t, "bbb", string(pair.Key))
	})

	t.Run("SearchModeKey で存在しないキーを検索すると挿入位置から開始される", func(t *testing.T) {
		// GIVEN
		bt, bpm := setupBTree(t)
		bt.mustInsert(bpm, "aaa", "v1")
		bt.mustInsert(bpm, "ccc", "v3")
		bt.mustInsert(bpm, "eee", "v5")

		// WHEN: "bbb" は存在しないが、"ccc" の位置から開始される
		iter, err := bt.Search(bpm, SearchModeKey{Key: []byte("bbb")})

		// THEN
		assert.NoError(t, err)
		pair, ok := iter.Get()
		assert.True(t, ok)
		assert.Equal(t, "ccc", string(pair.Key))
	})

	t.Run("SearchModeKey で最大キーより大きいキーを検索すると結果が空になる", func(t *testing.T) {
		// GIVEN
		bt, bpm := setupBTree(t)
		bt.mustInsert(bpm, "aaa", "v1")
		bt.mustInsert(bpm, "bbb", "v2")

		// WHEN
		iter, err := bt.Search(bpm, SearchModeKey{Key: []byte("zzz")})

		// THEN
		assert.NoError(t, err)
		_, ok := iter.Get()
		assert.False(t, ok)
	})

	t.Run("SearchModeKey で削除済みキーを検索すると次のキーから開始される", func(t *testing.T) {
		// GIVEN
		bt, bpm := setupBTree(t)
		bt.mustInsert(bpm, "aaa", "v1")
		bt.mustInsert(bpm, "bbb", "v2")
		bt.mustInsert(bpm, "ccc", "v3")
		err := bt.Delete(bpm, []byte("bbb"))
		assert.NoError(t, err)

		// WHEN: 削除済みの "bbb" で検索
		iter, err := bt.Search(bpm, SearchModeKey{Key: []byte("bbb")})

		// THEN: 次のキー "ccc" から開始される
		assert.NoError(t, err)
		pair, ok := iter.Get()
		assert.True(t, ok)
		assert.Equal(t, "ccc", string(pair.Key))
	})

	t.Run("分割が発生した B+Tree で SearchModeKey が正しく動作する", func(t *testing.T) {
		// GIVEN: 多数のペアを挿入してノード分割を発生させる
		bt, bpm := setupBTree(t)
		numPairs := 100
		for i := range numPairs {
			key := fmt.Sprintf("key%03d", i)
			val := fmt.Sprintf("val%03d", i)
			bt.mustInsert(bpm, key, val)
		}

		// WHEN: 中間のキーで検索
		iter, err := bt.Search(bpm, SearchModeKey{Key: []byte("key050")})

		// THEN: key050 から順に取得できる
		assert.NoError(t, err)
		pair, ok, err := iter.Next(bpm)
		assert.NoError(t, err)
		assert.True(t, ok)
		assert.Equal(t, "key050", string(pair.Key))

		// 次のペアも正しく取得できる
		pair, ok, err = iter.Next(bpm)
		assert.NoError(t, err)
		assert.True(t, ok)
		assert.Equal(t, "key051", string(pair.Key))
	})
}

func TestDelete(t *testing.T) {
	t.Run("リーフノードのみの B+Tree からペアを削除できる", func(t *testing.T) {
		// GIVEN
		bt, bpm := setupBTree(t)
		bt.mustInsert(bpm, "key1", "val1")
		bt.mustInsert(bpm, "key2", "val2")
		bt.mustInsert(bpm, "key3", "val3")

		// WHEN
		err := bt.Delete(bpm, []byte("key2"))

		// THEN
		assert.NoError(t, err)
		pairs := bt.collectAllPairs(bpm)
		assert.Equal(t, 2, len(pairs))
		assert.Equal(t, "key1", string(pairs[0].Key))
		assert.Equal(t, "key3", string(pairs[1].Key))
	})

	t.Run("存在しないキーを削除するとエラーが返る", func(t *testing.T) {
		// GIVEN
		bt, bpm := setupBTree(t)
		bt.mustInsert(bpm, "key1", "val1")

		// WHEN
		err := bt.Delete(bpm, []byte("nonexistent"))

		// THEN
		assert.ErrorIs(t, err, ErrKeyNotFound)
	})

	t.Run("すべてのペアを削除できる", func(t *testing.T) {
		// GIVEN
		bt, bpm := setupBTree(t)
		bt.mustInsert(bpm, "key1", "val1")
		bt.mustInsert(bpm, "key2", "val2")

		// WHEN
		err1 := bt.Delete(bpm, []byte("key1"))
		err2 := bt.Delete(bpm, []byte("key2"))

		// THEN
		assert.NoError(t, err1)
		assert.NoError(t, err2)
		pairs := bt.collectAllPairs(bpm)
		assert.Equal(t, 0, len(pairs))
	})

	t.Run("分割が発生した B+Tree から削除できる", func(t *testing.T) {
		// GIVEN: 多数のペアを挿入してノード分割を発生させる
		bt, bpm := setupBTree(t)
		numPairs := 100
		for i := range numPairs {
			key := fmt.Sprintf("key%03d", i)
			val := fmt.Sprintf("val%03d", i)
			bt.mustInsert(bpm, key, val)
		}

		// WHEN: 一部のペアを削除
		for i := 0; i < numPairs; i += 2 {
			key := fmt.Sprintf("key%03d", i)
			err := bt.Delete(bpm, []byte(key))
			assert.NoError(t, err)
		}

		// THEN: 残りのペアが正しく取得できる
		pairs := bt.collectAllPairs(bpm)
		assert.Equal(t, numPairs/2, len(pairs))
		for i, pair := range pairs {
			expectedKey := fmt.Sprintf("key%03d", i*2+1)
			assert.Equal(t, expectedKey, string(pair.Key))
		}
	})

	t.Run("すべてのペアを順次削除しても B+Tree が壊れない", func(t *testing.T) {
		// GIVEN: 多数のペアを挿入
		bt, bpm := setupBTree(t)
		numPairs := 50
		for i := range numPairs {
			key := fmt.Sprintf("key%03d", i)
			val := fmt.Sprintf("val%03d", i)
			bt.mustInsert(bpm, key, val)
		}

		// WHEN: 全ペアを先頭から順に削除
		for i := range numPairs {
			key := fmt.Sprintf("key%03d", i)
			err := bt.Delete(bpm, []byte(key))
			assert.NoError(t, err)
		}

		// THEN: B+Tree が空になる
		pairs := bt.collectAllPairs(bpm)
		assert.Equal(t, 0, len(pairs))
	})

	t.Run("末尾から逆順に全ペアを削除しても B+Tree が壊れない", func(t *testing.T) {
		// GIVEN: 多数のペアを挿入
		bt, bpm := setupBTree(t)
		numPairs := 50
		for i := range numPairs {
			key := fmt.Sprintf("key%03d", i)
			val := fmt.Sprintf("val%03d", i)
			bt.mustInsert(bpm, key, val)
		}

		// WHEN: 全ペアを末尾から逆順に削除
		for i := numPairs - 1; i >= 0; i-- {
			key := fmt.Sprintf("key%03d", i)
			err := bt.Delete(bpm, []byte(key))
			assert.NoError(t, err)
		}

		// THEN: B+Tree が空になる
		pairs := bt.collectAllPairs(bpm)
		assert.Equal(t, 0, len(pairs))
	})

	t.Run("不規則な順序で全ペアを削除しても B+Tree が壊れない", func(t *testing.T) {
		// GIVEN: 多数のペアを挿入
		bt, bpm := setupBTree(t)
		numPairs := 50
		keys := make([]string, numPairs)
		for i := range numPairs {
			keys[i] = fmt.Sprintf("key%03d", i)
			val := fmt.Sprintf("val%03d", i)
			bt.mustInsert(bpm, keys[i], val)
		}

		// WHEN: 中間→先頭→末尾の順で削除 (決定論的な不規則順序)
		deleteOrder := make([]string, 0, numPairs)
		for i := numPairs / 2; i < numPairs; i++ {
			deleteOrder = append(deleteOrder, keys[i])
		}
		for i := 0; i < numPairs/2; i++ {
			deleteOrder = append(deleteOrder, keys[i])
		}
		for _, key := range deleteOrder {
			err := bt.Delete(bpm, []byte(key))
			assert.NoError(t, err)
		}

		// THEN: B+Tree が空になる
		pairs := bt.collectAllPairs(bpm)
		assert.Equal(t, 0, len(pairs))
	})

	t.Run("空のツリーから削除するとエラーが返る", func(t *testing.T) {
		// GIVEN
		bt, bpm := setupBTree(t)

		// WHEN
		err := bt.Delete(bpm, []byte("nonexistent"))

		// THEN
		assert.ErrorIs(t, err, ErrKeyNotFound)
	})

	t.Run("削除後に新たに挿入できる", func(t *testing.T) {
		// GIVEN
		bt, bpm := setupBTree(t)
		bt.mustInsert(bpm, "key1", "val1")
		bt.mustInsert(bpm, "key2", "val2")
		err := bt.Delete(bpm, []byte("key1"))
		assert.NoError(t, err)

		// WHEN
		err = bt.Insert(bpm, node.NewPair([]byte("key3"), []byte("val3")))

		// THEN
		assert.NoError(t, err)
		pairs := bt.collectAllPairs(bpm)
		assert.Equal(t, 2, len(pairs))
		assert.Equal(t, "key2", string(pairs[0].Key))
		assert.Equal(t, "key3", string(pairs[1].Key))
	})
}

func TestNewBTree(t *testing.T) {
	t.Run("既存の B+Tree を NewBTree で開いてデータを読み取れる", func(t *testing.T) {
		// GIVEN: CreateBTree でツリーを作成しペアを挿入する
		bt, bpm := setupBTree(t)
		bt.mustInsert(bpm, "aaa", "v1")
		bt.mustInsert(bpm, "bbb", "v2")
		bt.mustInsert(bpm, "ccc", "v3")

		// WHEN: 同じ metaPageId で NewBTree を呼ぶ
		bt2 := NewBTree(bt.MetaPageId)

		// THEN: 挿入したペアがすべて取得できる
		pairs := bt2.collectAllPairs(bpm)
		assert.Equal(t, 3, len(pairs))
		assert.Equal(t, "aaa", string(pairs[0].Key))
		assert.Equal(t, "bbb", string(pairs[1].Key))
		assert.Equal(t, "ccc", string(pairs[2].Key))
	})
}

// テスト用の B+Tree とバッファプールマネージャをセットアップする
func setupBTree(t *testing.T) (*BTree, *bufferpool.BufferPoolManager) {
	t.Helper()
	tmpdir := t.TempDir()
	path := filepath.Join(tmpdir, "btree_test.db")
	fileId := page.FileId(0)
	dm, err := disk.NewDiskManager(fileId, path)
	if err != nil {
		t.Fatalf("DiskManager の作成に失敗: %v", err)
	}
	metaPageId := dm.AllocatePage()

	bpm := bufferpool.NewBufferPoolManager(100)
	bpm.RegisterDiskManager(fileId, dm)

	bt, err := CreateBTree(bpm, metaPageId)
	if err != nil {
		t.Fatalf("B+Tree の作成に失敗: %v", err)
	}
	return bt, bpm
}

// ペアを挿入するヘルパー (エラー時は panic)
func (bt *BTree) mustInsert(bpm *bufferpool.BufferPoolManager, key, value string) {
	err := bt.Insert(bpm, node.NewPair([]byte(key), []byte(value)))
	if err != nil {
		panic(fmt.Sprintf("Insert に失敗: %v", err))
	}
}

// B+Tree の全ペアをイテレータで収集する
func (bt *BTree) collectAllPairs(bpm *bufferpool.BufferPoolManager) []node.Pair {
	iter, err := bt.Search(bpm, SearchModeStart{})
	if err != nil {
		panic(fmt.Sprintf("Search に失敗: %v", err))
	}
	var pairs []node.Pair
	for {
		pair, ok, err := iter.Next(bpm)
		if err != nil {
			panic(fmt.Sprintf("Next に失敗: %v", err))
		}
		if !ok {
			break
		}
		// ペアのキーと値をコピー (underlying data への参照を避ける)
		keyCopy := make([]byte, len(pair.Key))
		copy(keyCopy, pair.Key)
		valCopy := make([]byte, len(pair.Value))
		copy(valCopy, pair.Value)
		pairs = append(pairs, node.NewPair(keyCopy, valCopy))
	}
	return pairs
}
