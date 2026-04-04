package executor

import (
	"minesql/internal/storage/access"
	"minesql/internal/storage/handler"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestProject(t *testing.T) {
	t.Run("特定のカラムだけを取得できる", func(t *testing.T) {
		// GIVEN
		setupExecutorTestTable(t)
		defer handler.Reset()

		// テーブルアクセスメソッドを取得
		tbl, err := getTable("users")
		assert.NoError(t, err)

		// WHEN: first_name (pos=1) と last_name (pos=2) のみ取得
		records := collectAll(t, NewProject(
			NewTableScan(
				0, nil, tbl,
				access.RecordSearchModeStart{},
				func(record Record) bool { return true },
			),
			[]uint16{1, 2},
		))

		// THEN
		assert.Equal(t, 5, len(records))
		assert.Equal(t, Record{[]byte("Eve"), []byte("Brown")}, records[0])
		assert.Equal(t, Record{[]byte("Dave"), []byte("Miller")}, records[1])
		assert.Equal(t, Record{[]byte("Bob"), []byte("Johnson")}, records[2])
		assert.Equal(t, Record{[]byte("Charlie"), []byte("Williams")}, records[3])
		assert.Equal(t, Record{[]byte("Alice"), []byte("Smith")}, records[4])
	})

	t.Run("1 カラムだけを取得できる", func(t *testing.T) {
		// GIVEN
		setupExecutorTestTable(t)
		defer handler.Reset()

		// テーブルアクセスメソッドを取得
		tbl, err := getTable("users")
		assert.NoError(t, err)

		// WHEN: first_name (pos=1) のみ取得
		records := collectAll(t, NewProject(
			NewTableScan(
				0, nil, tbl,
				access.RecordSearchModeStart{},
				func(record Record) bool { return true },
			),
			[]uint16{1},
		))

		// THEN
		assert.Equal(t, 5, len(records))
		assert.Equal(t, Record{[]byte("Eve")}, records[0])
		assert.Equal(t, Record{[]byte("Dave")}, records[1])
		assert.Equal(t, Record{[]byte("Bob")}, records[2])
		assert.Equal(t, Record{[]byte("Charlie")}, records[3])
		assert.Equal(t, Record{[]byte("Alice")}, records[4])
	})

	t.Run("カラムの順序を入れ替えて取得できる", func(t *testing.T) {
		// GIVEN
		setupExecutorTestTable(t)
		defer handler.Reset()

		// テーブルアクセスメソッドを取得
		tbl, err := getTable("users")
		assert.NoError(t, err)

		// WHEN: last_name (pos=2), id (pos=0) の順で取得
		records := collectAll(t, NewProject(
			NewTableScan(
				0, nil, tbl,
				access.RecordSearchModeStart{},
				func(record Record) bool { return true },
			),
			[]uint16{2, 0},
		))

		// THEN
		assert.Equal(t, 5, len(records))
		assert.Equal(t, Record{[]byte("Brown"), []byte("v")}, records[0])
		assert.Equal(t, Record{[]byte("Miller"), []byte("w")}, records[1])
	})

	t.Run("Filter と組み合わせて使用できる", func(t *testing.T) {
		// GIVEN
		setupExecutorTestTable(t)
		defer handler.Reset()

		// テーブルアクセスメソッドを取得
		tbl, err := getTable("users")
		assert.NoError(t, err)

		// WHEN: first_name が "Alice" のレコードから first_name と last_name を取得
		records := collectAll(t, NewProject(
			NewFilter(
				NewTableScan(
					0, nil, tbl,
					access.RecordSearchModeStart{},
					func(record Record) bool { return true },
				),
				func(record Record) bool {
					return string(record[1]) == "Alice"
				},
			),
			[]uint16{1, 2},
		))

		// THEN
		assert.Equal(t, 1, len(records))
		assert.Equal(t, Record{[]byte("Alice"), []byte("Smith")}, records[0])
	})

	t.Run("InnerExecutor が空の場合は空のレコードを返す", func(t *testing.T) {
		// GIVEN
		setupExecutorTestTable(t)
		defer handler.Reset()

		// テーブルアクセスメソッドを取得
		tbl, err := getTable("users")
		assert.NoError(t, err)

		// WHEN: 存在しない条件でフィルタした結果を射影
		records := collectAll(t, NewProject(
			NewFilter(
				NewTableScan(
					0, nil, tbl,
					access.RecordSearchModeStart{},
					func(record Record) bool { return true },
				),
				func(record Record) bool {
					return string(record[1]) == "NonExistent"
				},
			),
			[]uint16{1, 2},
		))

		// THEN
		assert.Equal(t, 0, len(records))
	})
}
