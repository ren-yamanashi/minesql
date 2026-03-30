package executor

import (
	"testing"

	"minesql/internal/engine"
	"minesql/internal/storage/access"
	"minesql/internal/storage/catalog"

	"github.com/stretchr/testify/assert"
)

func TestDelete(t *testing.T) {
	t.Run("正常に Delete Executor を生成できる", func(t *testing.T) {
		// GIVEN
		var trxId engine.TrxId = 1
		iterator := NewTableScan(
			nil,
			access.RecordSearchModeStart{},
			func(record Record) bool { return true },
		)

		// WHEN
		del := NewDelete(trxId, nil, iterator)

		// THEN
		assert.NotNil(t, del)
		assert.Nil(t, del.table)
		assert.NotNil(t, del.InnerExecutor)
	})

	t.Run("SearchTable を使って全レコードを削除できる", func(t *testing.T) {
		// GIVEN
		tmpdir := t.TempDir()
		InitStorageEngineForTest(t, tmpdir)
		defer engine.Reset()

		var trxId engine.TrxId = 1
		tbl, err := getTableAccessMethod("users")
		assert.NoError(t, err)

		del := NewDelete(trxId, tbl, NewTableScan(
			tbl,
			access.RecordSearchModeStart{},
			func(record Record) bool { return true },
		))

		// WHEN
		_, err = del.Next()
		assert.NoError(t, err)

		// THEN: 削除が成功する
		assert.NoError(t, err)

		// THEN: テーブルが空になっている
		scan := NewTableScan(
			tbl,
			access.RecordSearchModeStart{},
			func(record Record) bool { return true },
		)
		results, err := fetchAll(scan)
		assert.NoError(t, err)
		assert.Equal(t, 0, len(results))
	})

	t.Run("条件付きで一部のレコードを削除できる", func(t *testing.T) {
		// GIVEN
		tmpdir := t.TempDir()
		InitStorageEngineForTest(t, tmpdir)
		defer engine.Reset()

		var trxId engine.TrxId = 1

		// テーブルアクセスメソッドを取得
		tbl, err := getTableAccessMethod("users")
		assert.NoError(t, err)

		// プライマリキーが "c" 未満のレコードを削除対象とする
		iterator := NewTableScan(
			tbl,
			access.RecordSearchModeStart{},
			func(record Record) bool {
				return string(record[0]) < "c"
			},
		)

		del := NewDelete(trxId, tbl, iterator)

		// WHEN
		_, err = del.Next()
		assert.NoError(t, err)

		// THEN: 削除が成功する
		assert.NoError(t, err)

		// THEN: "c" 以降のレコードが残っている
		scan := NewTableScan(
			tbl,
			access.RecordSearchModeStart{},
			func(record Record) bool { return true },
		)
		results, err := fetchAll(scan)
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

		var trxId engine.TrxId = 1
		tbl, err := getTableAccessMethod("users")
		assert.NoError(t, err)

		// first_name が "Bob" のレコードを削除
		iterator := NewFilter(
			NewTableScan(
				tbl,
				access.RecordSearchModeStart{},
				func(record Record) bool { return true },
			),
			func(record Record) bool {
				return string(record[1]) == "Bob"
			},
		)
		del := NewDelete(trxId, tbl, iterator)

		// WHEN
		_, err = del.Next()

		// THEN: 削除が成功する
		assert.NoError(t, err)

		// THEN: "Bob" 以外のレコードが残っている
		scan := NewTableScan(
			tbl,
			access.RecordSearchModeStart{},
			func(record Record) bool { return true },
		)
		results, err := fetchAll(scan)
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

		// テーブルアクセスメソッドを取得
		tbl, err := getTableAccessMethod("users")
		assert.NoError(t, err)

		// インデックスアクセスメソッドを取得
		idx, err := tbl.GetUniqueIndexByName("last_name")
		assert.NoError(t, err)

		// プライマリキーが "a" のレコードを削除 (last_name = "Doe")
		iterator := NewTableScan(
			tbl,
			access.RecordSearchModeKey{Key: [][]byte{[]byte("a")}},
			func(record Record) bool {
				return string(record[0]) == "a"
			},
		)

		// Delete Executor を作成
		var trxId engine.TrxId = 1
		del := NewDelete(trxId, tbl, iterator)

		// WHEN
		_, err = del.Next()

		// THEN: 削除が成功する
		assert.NoError(t, err)

		// THEN: ユニークインデックスからも "Doe" が削除されている
		indexScan := NewIndexScan(
			tbl,
			idx,
			access.RecordSearchModeStart{},
			func(record Record) bool { return true },
		)
		results, err := fetchAll(indexScan)
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

		var trxId engine.TrxId = 1
		createTableForTest(t, "empty_table", nil, []*ColumnParam{
			{Name: "id", Type: catalog.ColumnTypeString},
			{Name: "value", Type: catalog.ColumnTypeString},
		})

		tbl, err := getTableAccessMethod("empty_table")
		assert.NoError(t, err)
		del := NewDelete(trxId, tbl, NewTableScan(
			tbl,
			access.RecordSearchModeStart{},
			func(record Record) bool { return true },
		))

		// WHEN
		_, err = del.Next()

		// THEN
		assert.NoError(t, err)
	})
}
