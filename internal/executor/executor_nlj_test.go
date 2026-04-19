package executor

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// nljMockExecutor はテスト用の固定レコードを返す Executor
type nljMockExecutor struct {
	records []Record
	pos     int
}

func newMockExecutor(records []Record) *nljMockExecutor {
	return &nljMockExecutor{records: records}
}

func (m *nljMockExecutor) Next() (Record, error) {
	if m.pos >= len(m.records) {
		return nil, nil
	}
	r := m.records[m.pos]
	m.pos++
	return r, nil
}

func TestNestedLoopJoin(t *testing.T) {
	t.Run("左 3 行 × 右 2 行で 6 行出力される", func(t *testing.T) {
		// GIVEN
		leftRecords := []Record{
			{[]byte("a1"), []byte("a2")},
			{[]byte("b1"), []byte("b2")},
			{[]byte("c1"), []byte("c2")},
		}
		rightRecords := []Record{
			{[]byte("x1")},
			{[]byte("x2")},
		}
		buildRight := func(leftRecord Record) (Executor, error) {
			return newMockExecutor(rightRecords), nil
		}
		nlj := NewNestedLoopJoin(newMockExecutor(leftRecords), buildRight)

		// WHEN
		var results []Record
		for {
			r, err := nlj.Next()
			require.NoError(t, err)
			if r == nil {
				break
			}
			results = append(results, r)
		}

		// THEN
		assert.Len(t, results, 6)
		// 1 行目: a1, a2, x1
		assert.Equal(t, "a1", string(results[0][0]))
		assert.Equal(t, "a2", string(results[0][1]))
		assert.Equal(t, "x1", string(results[0][2]))
		// 2 行目: a1, a2, x2
		assert.Equal(t, "x2", string(results[1][2]))
		// 5 行目: c1, c2, x1
		assert.Equal(t, "c1", string(results[4][0]))
	})

	t.Run("左 0 行で 0 行出力される", func(t *testing.T) {
		// GIVEN
		buildRight := func(leftRecord Record) (Executor, error) {
			return newMockExecutor([]Record{{[]byte("x")}}), nil
		}
		nlj := NewNestedLoopJoin(newMockExecutor(nil), buildRight)

		// WHEN
		r, err := nlj.Next()

		// THEN
		require.NoError(t, err)
		assert.Nil(t, r)
	})

	t.Run("右 0 行で 0 行出力される", func(t *testing.T) {
		// GIVEN
		leftRecords := []Record{
			{[]byte("a1")},
			{[]byte("b1")},
		}
		buildRight := func(leftRecord Record) (Executor, error) {
			return newMockExecutor(nil), nil
		}
		nlj := NewNestedLoopJoin(newMockExecutor(leftRecords), buildRight)

		// WHEN
		r, err := nlj.Next()

		// THEN
		require.NoError(t, err)
		assert.Nil(t, r)
	})

	t.Run("右が条件一致 1 行のみの場合 eq_ref 相当の動作をする", func(t *testing.T) {
		// GIVEN: 左の値に応じて右を 1 行返す (eq_ref シミュレーション)
		leftRecords := []Record{
			{[]byte("1"), []byte("Alice")},
			{[]byte("2"), []byte("Bob")},
		}
		buildRight := func(leftRecord Record) (Executor, error) {
			// 左の id に対応する注文を 1 行だけ返す
			id := string(leftRecord[0])
			switch id {
			case "1":
				return newMockExecutor([]Record{{[]byte("order_100"), []byte("1")}}), nil
			case "2":
				return newMockExecutor([]Record{{[]byte("order_200"), []byte("2")}}), nil
			default:
				return newMockExecutor(nil), nil
			}
		}
		nlj := NewNestedLoopJoin(newMockExecutor(leftRecords), buildRight)

		// WHEN
		var results []Record
		for {
			r, err := nlj.Next()
			require.NoError(t, err)
			if r == nil {
				break
			}
			results = append(results, r)
		}

		// THEN: 左 2 行 × 右 1 行 = 2 行
		assert.Len(t, results, 2)
		// 結合レコード: [id, name, order_id, user_id]
		assert.Equal(t, "1", string(results[0][0]))
		assert.Equal(t, "Alice", string(results[0][1]))
		assert.Equal(t, "order_100", string(results[0][2]))
		assert.Equal(t, "1", string(results[0][3]))
		assert.Equal(t, "2", string(results[1][0]))
		assert.Equal(t, "Bob", string(results[1][1]))
		assert.Equal(t, "order_200", string(results[1][2]))
	})

	t.Run("一部の左行に右が一致しない場合はスキップされる", func(t *testing.T) {
		// GIVEN: 左 3 行のうち 2 行のみ右と一致
		leftRecords := []Record{
			{[]byte("1")},
			{[]byte("2")},
			{[]byte("3")},
		}
		buildRight := func(leftRecord Record) (Executor, error) {
			id := string(leftRecord[0])
			if id == "2" {
				// id=2 に一致する右行はない
				return newMockExecutor(nil), nil
			}
			return newMockExecutor([]Record{{[]byte("match_" + id)}}), nil
		}
		nlj := NewNestedLoopJoin(newMockExecutor(leftRecords), buildRight)

		// WHEN
		var results []Record
		for {
			r, err := nlj.Next()
			require.NoError(t, err)
			if r == nil {
				break
			}
			results = append(results, r)
		}

		// THEN: id=1 と id=3 のみ一致 → 2 行
		assert.Len(t, results, 2)
		assert.Equal(t, "1", string(results[0][0]))
		assert.Equal(t, "match_1", string(results[0][1]))
		assert.Equal(t, "3", string(results[1][0]))
		assert.Equal(t, "match_3", string(results[1][1]))
	})
}
