package storage

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

// initAllAccessed は 8 スロットで全スロットを初期アクセス済みにしたポリシーを返すヘルパー
// 返却時のリスト: [7, 6, 5, 4, 3, 2, 1, 0], 全て old, midpoint=7, maxNew=5
func initAllAccessed() *LRU {
	policy := NewLRU(8)
	for i := range 8 {
		policy.Access(BufferId(i))
	}
	return policy
}

func TestLRU_Evict(t *testing.T) {
	t.Run("初期状態では末尾のスロットが追い出される", func(t *testing.T) {
		// GIVEN: 8 スロット、アクセスなし
		policy := NewLRU(8)

		// WHEN
		victim := policy.Evict()

		// THEN: 末尾のスロット 7 が追い出される
		assert.Equal(t, BufferId(7), victim)
	})

	t.Run("新しいページは midpoint に配置され、末尾のページが追い出される", func(t *testing.T) {
		// GIVEN: 8 スロット、スロット 0, 1 を初期アクセス
		// Access(0): midpoint に配置 (移動なし)
		// Access(1): midpoint に配置 (0 の前に挿入)
		// リスト: [1, 0, 2, 3, 4, 5, 6, 7], 全て old
		policy := NewLRU(8)
		policy.Access(BufferId(0))
		policy.Access(BufferId(1))

		// WHEN
		victim := policy.Evict()

		// THEN: 末尾のスロット 7 が追い出される (新しいページは先頭に行かない)
		assert.Equal(t, BufferId(7), victim)
	})

	t.Run("old ページの再アクセスで new に昇格した後、old 末尾が追い出される", func(t *testing.T) {
		// GIVEN: 全スロットを初期アクセス後、スロット 0 を再アクセス
		// リスト: [7,6,5,4,3,2,1,0], 全て old, midpoint=7
		// Access(0): new に昇格
		// new=[0], old=[7,6,5,4,3,2,1], midpoint=7
		policy := initAllAccessed()
		policy.Access(BufferId(0))

		// WHEN
		victim := policy.Evict()

		// THEN: old 末尾のスロット 1 が追い出される
		assert.Equal(t, BufferId(1), victim)
	})
}

func TestLRU_Access(t *testing.T) {
	t.Run("新規ページは midpoint に配置される", func(t *testing.T) {
		// GIVEN: 全スロットを初期アクセス後、スロット 0, 1, 2 を再アクセスして new に昇格
		// new=[2,1,0], old=[7,6,5,4,3], midpoint=7
		// Evict → スロット 3 が追い出される (isNew=true)
		policy := initAllAccessed()
		policy.Access(BufferId(0))
		policy.Access(BufferId(1))
		policy.Access(BufferId(2))
		victim := policy.Evict()
		assert.Equal(t, BufferId(3), victim)

		// WHEN: 追い出されたスロット 3 に新しいページを読み込む
		policy.Access(BufferId(3))

		// THEN: midpoint に配置されるため、old 末尾の 4 が次の追い出し対象
		victim2 := policy.Evict()
		assert.Equal(t, BufferId(4), victim2)
	})

	t.Run("old ページの再アクセスで new の先頭に昇格する", func(t *testing.T) {
		// GIVEN: 全スロットを初期アクセス
		// リスト: [7,6,5,4,3,2,1,0], 全て old, midpoint=7
		policy := initAllAccessed()

		// WHEN: スロット 0 (old 末尾) を再アクセス
		policy.Access(BufferId(0))

		// THEN: new の先頭に昇格し、old 末尾だったスロット 1 が追い出し対象になる
		victim := policy.Evict()
		assert.Equal(t, BufferId(1), victim)
	})

	t.Run("new サブリストが最大長を超えると old に降格される", func(t *testing.T) {
		// GIVEN: 8 スロット (maxNew=5)
		// 全スロットを初期アクセス後、スロット 0〜5 を順に再アクセス
		// Access(0)〜Access(4): new に昇格 (newLen=5 = maxNew)
		// Access(5): newLen が 6 > maxNew=5 となり、rebalance で new 末尾 (0) が old に降格
		// new=[5,4,3,2,1], old=[0,7,6], midpoint=0
		policy := initAllAccessed()
		for i := range 6 {
			policy.Access(BufferId(i))
		}

		// WHEN
		victim := policy.Evict()

		// THEN: old 末尾のスロット 6 が追い出される
		assert.Equal(t, BufferId(6), victim)
	})

	t.Run("new ページの再アクセスで new の先頭に移動する", func(t *testing.T) {
		// GIVEN: 全スロットを初期アクセス後、スロット 0, 1 を new に昇格
		// new=[1,0], old=[7,6,5,4,3,2], midpoint=7
		policy := initAllAccessed()
		policy.Access(BufferId(0))
		policy.Access(BufferId(1))

		// WHEN: new 内のスロット 0 を再アクセス (new 先頭に移動)
		policy.Access(BufferId(0))

		// THEN: スロット 0 が new 先頭に移動しても、追い出し対象は old 末尾のスロット 2
		victim := policy.Evict()
		assert.Equal(t, BufferId(2), victim)
	})
}

func TestLRUPolicy_Remove(t *testing.T) {
	t.Run("Remove したページが優先的に追い出される", func(t *testing.T) {
		// GIVEN: 全スロットを初期アクセス後、スロット 5 を Remove
		// リスト: [7,6,5,4,3,2,1,0], 全て old
		// Remove(5): スロット 5 を old 末尾に移動
		// リスト: [7,6,4,3,2,1,0,5]
		policy := initAllAccessed()
		policy.Remove(BufferId(5))

		// WHEN
		victim := policy.Evict()

		// THEN: スロット 5 が末尾にあるため追い出される
		assert.Equal(t, BufferId(5), victim)
	})

	t.Run("new ページを Remove すると old 末尾に移動する", func(t *testing.T) {
		// GIVEN: 全スロットを初期アクセス後、スロット 0 を new に昇格、その後 Remove
		// new=[0], old=[7,6,5,4,3,2,1], midpoint=7
		// Remove(0): スロット 0 を old 末尾に移動
		policy := initAllAccessed()
		policy.Access(BufferId(0))
		policy.Remove(BufferId(0))

		// WHEN
		victim := policy.Evict()

		// THEN: スロット 0 が old 末尾に移動したため追い出される
		assert.Equal(t, BufferId(0), victim)
	})
}
