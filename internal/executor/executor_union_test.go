package executor

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type mockExecutor struct {
	records []Record
	index   int
}

func (m *mockExecutor) Next() (Record, error) {
	if m.index >= len(m.records) {
		return nil, nil
	}
	r := m.records[m.index]
	m.index++
	return r, nil
}

func TestUnion(t *testing.T) {
	t.Run("複数の Executor の結果を結合する", func(t *testing.T) {
		// GIVEN
		exec1 := &mockExecutor{records: []Record{
			{[]byte("1"), []byte("Alice")},
			{[]byte("2"), []byte("Bob")},
		}}
		exec2 := &mockExecutor{records: []Record{
			{[]byte("3"), []byte("Charlie")},
		}}
		union := NewUnion([]Executor{exec1, exec2})

		// WHEN
		var results []Record
		for {
			record, err := union.Next()
			require.NoError(t, err)
			if record == nil {
				break
			}
			results = append(results, record)
		}

		// THEN
		assert.Len(t, results, 3)
		assert.Equal(t, "1", string(results[0][0]))
		assert.Equal(t, "2", string(results[1][0]))
		assert.Equal(t, "3", string(results[2][0]))
	})

	t.Run("重複するレコードを除去する", func(t *testing.T) {
		// GIVEN: 両方の Executor に同じレコードが含まれる
		exec1 := &mockExecutor{records: []Record{
			{[]byte("1"), []byte("Alice")},
			{[]byte("2"), []byte("Bob")},
		}}
		exec2 := &mockExecutor{records: []Record{
			{[]byte("2"), []byte("Bob")},
			{[]byte("3"), []byte("Charlie")},
		}}
		union := NewUnion([]Executor{exec1, exec2})

		// WHEN
		var results []Record
		for {
			record, err := union.Next()
			require.NoError(t, err)
			if record == nil {
				break
			}
			results = append(results, record)
		}

		// THEN: 重複する (2, Bob) は 1 回だけ返される
		assert.Len(t, results, 3)
		assert.Equal(t, "1", string(results[0][0]))
		assert.Equal(t, "2", string(results[1][0]))
		assert.Equal(t, "3", string(results[2][0]))
	})

	t.Run("空の Executor を含む場合", func(t *testing.T) {
		// GIVEN
		exec1 := &mockExecutor{records: nil}
		exec2 := &mockExecutor{records: []Record{
			{[]byte("1"), []byte("Alice")},
		}}
		exec3 := &mockExecutor{records: nil}
		union := NewUnion([]Executor{exec1, exec2, exec3})

		// WHEN
		var results []Record
		for {
			record, err := union.Next()
			require.NoError(t, err)
			if record == nil {
				break
			}
			results = append(results, record)
		}

		// THEN
		assert.Len(t, results, 1)
		assert.Equal(t, "1", string(results[0][0]))
	})

	t.Run("全ての Executor が空の場合", func(t *testing.T) {
		// GIVEN
		union := NewUnion([]Executor{
			&mockExecutor{records: nil},
			&mockExecutor{records: nil},
		})

		// WHEN
		record, err := union.Next()

		// THEN
		require.NoError(t, err)
		assert.Nil(t, record)
	})
}
