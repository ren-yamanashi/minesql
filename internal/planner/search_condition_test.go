package planner

import (
	"minesql/internal/executor"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestOperatorToCondition(t *testing.T) {
	// operatorToCondition は Search のメソッドなので、ダミーの Search を使用する
	s := &Search{}

	t.Run("= 演算子が正しく動作する", func(t *testing.T) {
		// GIVEN
		record := executor.Record{[]byte("apple"), []byte("banana"), []byte("cherry")}

		// WHEN
		cond, err := s.operatorToCondition("=", 1, "banana")

		// THEN
		assert.NoError(t, err)
		assert.NotNil(t, cond)
		assert.True(t, cond(record))
		assert.False(t, cond(executor.Record{[]byte("apple"), []byte("orange"), []byte("cherry")}))
	})

	t.Run("!= 演算子が正しく動作する", func(t *testing.T) {
		// GIVEN
		record := executor.Record{[]byte("apple"), []byte("banana"), []byte("cherry")}

		// WHEN
		cond, err := s.operatorToCondition("!=", 1, "banana")

		// THEN
		assert.NoError(t, err)
		assert.NotNil(t, cond)
		assert.False(t, cond(record))
		assert.True(t, cond(executor.Record{[]byte("apple"), []byte("orange"), []byte("cherry")}))
	})

	t.Run("< 演算子が正しく動作する", func(t *testing.T) {
		// GIVEN
		// WHEN
		cond, err := s.operatorToCondition("<", 0, "c")

		// THEN
		assert.NoError(t, err)
		assert.NotNil(t, cond)
		assert.True(t, cond(executor.Record{[]byte("a"), []byte("banana")}))
		assert.True(t, cond(executor.Record{[]byte("b"), []byte("banana")}))
		assert.False(t, cond(executor.Record{[]byte("c"), []byte("banana")}))
		assert.False(t, cond(executor.Record{[]byte("d"), []byte("banana")}))
	})

	t.Run("<= 演算子が正しく動作する", func(t *testing.T) {
		// GIVEN
		// WHEN
		cond, err := s.operatorToCondition("<=", 0, "c")

		// THEN
		assert.NoError(t, err)
		assert.NotNil(t, cond)
		assert.True(t, cond(executor.Record{[]byte("a"), []byte("banana")}))
		assert.True(t, cond(executor.Record{[]byte("b"), []byte("banana")}))
		assert.True(t, cond(executor.Record{[]byte("c"), []byte("banana")}))
		assert.False(t, cond(executor.Record{[]byte("d"), []byte("banana")}))
	})

	t.Run("> 演算子が正しく動作する", func(t *testing.T) {
		// GIVEN
		// WHEN
		cond, err := s.operatorToCondition(">", 0, "c")

		// THEN
		assert.NoError(t, err)
		assert.NotNil(t, cond)
		assert.False(t, cond(executor.Record{[]byte("a"), []byte("banana")}))
		assert.False(t, cond(executor.Record{[]byte("b"), []byte("banana")}))
		assert.False(t, cond(executor.Record{[]byte("c"), []byte("banana")}))
		assert.True(t, cond(executor.Record{[]byte("d"), []byte("banana")}))
	})

	t.Run(">= 演算子が正しく動作する", func(t *testing.T) {
		// GIVEN
		// WHEN
		cond, err := s.operatorToCondition(">=", 0, "c")

		// THEN
		assert.NoError(t, err)
		assert.NotNil(t, cond)
		assert.False(t, cond(executor.Record{[]byte("a"), []byte("banana")}))
		assert.False(t, cond(executor.Record{[]byte("b"), []byte("banana")}))
		assert.True(t, cond(executor.Record{[]byte("c"), []byte("banana")}))
		assert.True(t, cond(executor.Record{[]byte("d"), []byte("banana")}))
	})

	t.Run("サポートされていない演算子の場合、エラーを返す", func(t *testing.T) {
		// GIVEN
		// WHEN
		cond, err := s.operatorToCondition("LIKE", 0, "pattern")

		// THEN
		assert.Error(t, err)
		assert.Nil(t, cond)
		assert.Contains(t, err.Error(), "unsupported operator")
		assert.Contains(t, err.Error(), "LIKE")
	})

	t.Run("異なる position で条件が正しく適用される", func(t *testing.T) {
		// GIVEN
		record := executor.Record{[]byte("1"), []byte("John"), []byte("Doe")}

		// WHEN: position 0 (id)
		cond0, err0 := s.operatorToCondition("=", 0, "1")
		// WHEN: position 1 (first_name)
		cond1, err1 := s.operatorToCondition("=", 1, "John")
		// WHEN: position 2 (last_name)
		cond2, err2 := s.operatorToCondition("=", 2, "Doe")

		// THEN
		assert.NoError(t, err0)
		assert.NoError(t, err1)
		assert.NoError(t, err2)
		assert.True(t, cond0(record))
		assert.True(t, cond1(record))
		assert.True(t, cond2(record))

		// 異なる値の場合は false
		assert.False(t, cond0(executor.Record{[]byte("2"), []byte("John"), []byte("Doe")}))
		assert.False(t, cond1(executor.Record{[]byte("1"), []byte("Jane"), []byte("Doe")}))
		assert.False(t, cond2(executor.Record{[]byte("1"), []byte("John"), []byte("Smith")}))
	})
}
