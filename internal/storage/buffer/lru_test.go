package buffer

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNewLru(t *testing.T) {
	t.Run("全スロットが OldSublist に配置される", func(t *testing.T) {
		// GIVEN / WHEN
		lru := newLru(8)

		// THEN
		assert.Equal(t, 8, lru.oldLen)
		assert.Equal(t, 0, lru.newLen)
		assert.Equal(t, 5, lru.maxNew) // 8 * 5 / 8 = 5
		assert.Equal(t, lru.head, lru.midpoint)
	})

	t.Run("全ノードが未使用状態で作成される", func(t *testing.T) {
		// GIVEN / WHEN
		lru := newLru(4)

		// THEN
		for _, node := range lru.nodeMap {
			assert.True(t, node.isUnused)
			assert.True(t, node.isOld)
		}
	})
}

func TestAccess(t *testing.T) {
	t.Run("未使用ノードにアクセスすると midpoint に配置される", func(t *testing.T) {
		// GIVEN
		lru := newLru(4)

		// WHEN
		lru.access(2)

		// THEN
		node := lru.nodeMap[2]
		assert.False(t, node.isUnused)
		assert.True(t, node.isOld)
		assert.Equal(t, lru.midpoint, node)
	})

	t.Run("OldSublist のノードに再アクセスすると NewSublist の先頭に昇格する", func(t *testing.T) {
		// GIVEN
		lru := newLru(8)
		lru.access(0) // unused → midpoint (Old)
		lru.access(1) // unused → midpoint (Old)

		// WHEN
		lru.access(0) // Old → New head

		// THEN
		node := lru.nodeMap[0]
		assert.False(t, node.isOld)
		assert.Equal(t, lru.head, node)
	})

	t.Run("NewSublist のノードに再アクセスすると NewSublist の先頭に移動する", func(t *testing.T) {
		// GIVEN
		lru := newLru(8)
		lru.access(0)
		lru.access(1)
		lru.access(0) // New head
		lru.access(1) // New head

		// WHEN
		lru.access(0) // New 内で先頭に移動

		// THEN
		assert.Equal(t, lru.head, lru.nodeMap[0])
	})

	t.Run("NewSublist の先頭ノードに再アクセスしても位置が変わらない", func(t *testing.T) {
		// GIVEN
		lru := newLru(8)
		lru.access(0)
		lru.access(0) // Old → New head

		// WHEN
		lru.access(0) // head と同じノード → early return

		// THEN
		assert.Equal(t, lru.head, lru.nodeMap[0])
	})

	t.Run("NewSublist が maxNew を超えるとリバランスが発生する", func(t *testing.T) {
		// GIVEN
		// size=8, maxNew=5
		lru := newLru(8)
		// 6 個のノードにアクセスして midpoint に配置
		for i := range 6 {
			lru.access(id(i))
		}
		// 全て Old → New に昇格 (6 個)
		for i := range 6 {
			lru.access(id(i))
		}

		// THEN
		// maxNew=5 なのでリバランスにより newLen <= 5
		assert.LessOrEqual(t, lru.newLen, lru.maxNew)
	})

	t.Run("size 1 で昇格時にリバランスが正しく動作する", func(t *testing.T) {
		// GIVEN
		lru := newLru(1) // maxNew=0, 全て Old

		// WHEN
		lru.access(0) // unused → midpoint
		lru.access(0) // Old → New (リバランスで midpoint.prev == nil のパスに入る)

		// THEN
		assert.LessOrEqual(t, lru.newLen, lru.maxNew+1)
	})

	t.Run("フルスキャンでホットページが追い出されない", func(t *testing.T) {
		// GIVEN
		// size=8, maxNew=5
		lru := newLru(8)

		// ページ 0, 1, 2 をホットページとして New に昇格
		for i := range 3 {
			lru.access(id(i)) // unused → midpoint
		}
		for i := range 3 {
			lru.access(id(i)) // Old → New head
		}

		// WHEN
		// ページ 3, 4, 5, 6, 7 をスキャン (midpoint に配置されるだけ)
		for i := 3; i < 8; i++ {
			lru.access(id(i)) // unused → midpoint
		}

		// THEN
		// ホットページ 0, 1, 2 はまだ New にいる
		for i := range 3 {
			node := lru.nodeMap[id(i)]
			assert.False(t, node.isOld)
		}
	})
}

func TestEvict(t *testing.T) {
	alwaysTrue := func(_ id) bool { return true }
	alwaysFalse := func(_ id) bool { return false }

	t.Run("常に true を返す callback ではリストの末尾の BufferId を返す", func(t *testing.T) {
		// GIVEN
		lru := newLru(4)

		// WHEN
		victim, err := lru.evict(alwaysTrue)

		// THEN
		assert.NoError(t, err)
		assert.Equal(t, lru.tail.bufferId, victim)
	})

	t.Run("追い出されたノードは未使用状態になる", func(t *testing.T) {
		// GIVEN
		lru := newLru(4)
		lru.access(0)

		// WHEN
		victim, err := lru.evict(alwaysTrue)

		// THEN
		assert.NoError(t, err)
		node := lru.nodeMap[victim]
		assert.True(t, node.isUnused)
	})

	t.Run("追い出し後に再アクセスすると midpoint に配置される", func(t *testing.T) {
		// GIVEN
		lru := newLru(4)
		lru.access(0)
		victim, err := lru.evict(alwaysTrue)
		assert.NoError(t, err)

		// WHEN
		lru.access(victim) // 未使用 → midpoint

		// THEN
		node := lru.nodeMap[victim]
		assert.False(t, node.isUnused)
		assert.Equal(t, lru.midpoint, node)
	})

	t.Run("常に false を返す callback では ErrAllPagesUnevictable を返す", func(t *testing.T) {
		// GIVEN
		lru := newLru(4)

		// WHEN
		_, err := lru.evict(alwaysFalse)

		// THEN
		assert.ErrorIs(t, err, ErrAllPagesUnevictable)
	})

	t.Run("特定ノードのみ true を返す callback ではそのノードが追い出される", func(t *testing.T) {
		// GIVEN
		lru := newLru(4)
		target := lru.tail.prev.bufferId
		canEvict := func(bufferId id) bool { return bufferId == target }

		// WHEN
		victim, err := lru.evict(canEvict)

		// THEN
		assert.NoError(t, err)
		assert.Equal(t, target, victim)
		assert.True(t, lru.nodeMap[victim].isUnused)
	})
}

func TestUndoEvict(t *testing.T) {
	alwaysTrue := func(_ id) bool { return true }

	t.Run("evict の結果を取り消すと isUnused が false に戻る", func(t *testing.T) {
		// GIVEN
		lru := newLru(4)
		lru.access(0)
		victim, err := lru.evict(alwaysTrue)
		assert.NoError(t, err)

		// WHEN
		lru.undoEvict(victim)

		// THEN
		node := lru.nodeMap[victim]
		assert.False(t, node.isUnused)
	})

	t.Run("undoEvict 後の再アクセスは midpoint ではなく通常のアクセスとして扱われる", func(t *testing.T) {
		// GIVEN
		lru := newLru(8)
		lru.access(0)
		victim, err := lru.evict(alwaysTrue)
		assert.NoError(t, err)
		lru.undoEvict(victim)

		// WHEN
		lru.access(victim)

		// THEN
		node := lru.nodeMap[victim]
		assert.False(t, node.isOld)
		assert.Equal(t, lru.head, node)
	})
}
