package executor

import (
	"testing"

	"minesql/internal/access"
	"minesql/internal/catalog"
	"minesql/internal/engine"

	"github.com/stretchr/testify/assert"
)

func TestNewDelete(t *testing.T) {
	t.Run("正常に Delete Executor を生成できる", func(t *testing.T) {
		// GIVEN
		tableName := "users"
		innerExecutor := NewSearchTable(
			tableName,
			access.RecordSearchModeStart{},
			func(record Record) bool { return true },
		)

		// WHEN
		del := NewDelete(tableName, innerExecutor)

		// THEN
		assert.NotNil(t, del)
		assert.Equal(t, tableName, del.tableName)
		assert.NotNil(t, del.InnerExecutor)
	})
}

func TestDeleteNext(t *testing.T) {
	t.Run("SearchTable を使って全レコードを削除できる", func(t *testing.T) {
		// GIVEN
		tmpdir := t.TempDir()
		InitStorageEngineForTest(t, tmpdir)
		defer engine.Reset()

		del := NewDelete("users", NewSearchTable(
			"users",
			access.RecordSearchModeStart{},
			func(record Record) bool { return true },
		))

		// WHEN
		_, err := del.Next()

		// THEN: 削除が成功する
		assert.NoError(t, err)

		// THEN: テーブルが空になっている
		scan := NewSearchTable(
			"users",
			access.RecordSearchModeStart{},
			func(record Record) bool { return true },
		)
		results, err := ExecutePlan(scan)
		assert.NoError(t, err)
		assert.Equal(t, 0, len(results))
	})

	t.Run("条件付きで一部のレコードを削除できる", func(t *testing.T) {
		// GIVEN
		tmpdir := t.TempDir()
		InitStorageEngineForTest(t, tmpdir)
		defer engine.Reset()

		// プライマリキーが "c" 未満のレコードを削除対象とする
		innerExecutor := NewSearchTable(
			"users",
			access.RecordSearchModeStart{},
			func(record Record) bool {
				return string(record[0]) < "c"
			},
		)
		del := NewDelete("users", innerExecutor)

		// WHEN
		_, err := del.Next()

		// THEN: 削除が成功する
		assert.NoError(t, err)

		// THEN: "c" 以降のレコードが残っている
		scan := NewSearchTable(
			"users",
			access.RecordSearchModeStart{},
			func(record Record) bool { return true },
		)
		results, err := ExecutePlan(scan)
		assert.NoError(t, err)
		assert.Equal(t, 3, len(results))
		assert.Equal(t, []byte("c"), results[0][0])
		assert.Equal(t, []byte("d"), results[1][0])
		assert.Equal(t, []byte("e"), results[2][0])
	})

	t.Run("Filter を組み合わせて特定の条件のレコードを削除できる", func(t *testing.T) {
		// GIVEN
		tmpdir := t.TempDir()
		InitStorageEngineForTest(t, tmpdir)
		defer engine.Reset()

		// first_name が "Bob" のレコードを削除
		innerExecutor := NewFilter(
			NewSearchTable(
				"users",
				access.RecordSearchModeStart{},
				func(record Record) bool { return true },
			),
			func(record Record) bool {
				return string(record[1]) == "Bob"
			},
		)
		del := NewDelete("users", innerExecutor)

		// WHEN
		_, err := del.Next()

		// THEN: 削除が成功する
		assert.NoError(t, err)

		// THEN: "Bob" 以外のレコードが残っている
		scan := NewSearchTable(
			"users",
			access.RecordSearchModeStart{},
			func(record Record) bool { return true },
		)
		results, err := ExecutePlan(scan)
		assert.NoError(t, err)
		assert.Equal(t, 4, len(results))
		for _, record := range results {
			assert.NotEqual(t, "Bob", string(record[1]))
		}
	})

	t.Run("削除後にユニークインデックスからも削除されている", func(t *testing.T) {
		// GIVEN
		tmpdir := t.TempDir()
		InitStorageEngineForTest(t, tmpdir)
		defer engine.Reset()

		// プライマリキーが "a" のレコードを削除 (last_name = "Doe")
		innerExecutor := NewSearchTable(
			"users",
			access.RecordSearchModeKey{Key: [][]byte{[]byte("a")}},
			func(record Record) bool {
				return string(record[0]) == "a"
			},
		)
		del := NewDelete("users", innerExecutor)

		// WHEN
		_, err := del.Next()

		// THEN: 削除が成功する
		assert.NoError(t, err)

		// THEN: ユニークインデックスからも "Doe" が削除されている
		indexScan := NewSearchIndex(
			"users",
			"last_name",
			access.RecordSearchModeStart{},
			func(record Record) bool { return true },
		)
		results, err := ExecutePlan(indexScan)
		assert.NoError(t, err)
		assert.Equal(t, 4, len(results))
		for _, record := range results {
			assert.NotEqual(t, "Doe", string(record[2]))
		}
	})

	t.Run("空のテーブルに対して削除しても正常に動作する", func(t *testing.T) {
		// GIVEN
		tmpdir := t.TempDir()
		initStorageManagerForTest(t)
		defer engine.Reset()
		_ = tmpdir

		createTableForTest(t, "empty_table", 1, nil, []*ColumnParam{
			{Name: "id", Type: catalog.ColumnTypeString},
			{Name: "value", Type: catalog.ColumnTypeString},
		})

		del := NewDelete("empty_table", NewSearchTable(
			"empty_table",
			access.RecordSearchModeStart{},
			func(record Record) bool { return true },
		))

		// WHEN
		_, err := del.Next()

		// THEN
		assert.NoError(t, err)
	})
}
