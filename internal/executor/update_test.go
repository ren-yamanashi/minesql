package executor

import (
	"minesql/internal/storage/handler"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNewUpdate(t *testing.T) {
	t.Run("正常に Update Executor を生成できる", func(t *testing.T) {
		// GIVEN
		var trxId handler.TrxId = 1
		setColumns := []SetColumn{
			{Pos: 1, Value: []byte("Jane")},
		}

		iterator := NewTableScan(
			nil,
			handler.SearchModeStart{},
			func(record Record) bool { return true },
		)

		// WHEN
		upd := NewUpdate(trxId, nil, setColumns, iterator)

		// THEN
		assert.NotNil(t, upd)
		assert.Nil(t, upd.table)
		assert.Equal(t, setColumns, upd.SetColumns)
		assert.NotNil(t, upd.InnerExecutor)
	})
}

func TestUpdate_Next(t *testing.T) {
	t.Run("全レコードの value を更新できる", func(t *testing.T) {
		// GIVEN
		tmpdir := t.TempDir()
		InitStorageEngineForTest(t, tmpdir)
		defer handler.Reset()

		var trxId handler.TrxId = 1

		// テーブルアクセスメソッドを取得
		tbl, err := getTableAccessMethod("users")
		assert.NoError(t, err)

		upd := NewUpdate(trxId, tbl, []SetColumn{
			{Pos: 1, Value: []byte("Updated")},
		}, NewTableScan(
			tbl,
			handler.SearchModeStart{},
			func(record Record) bool { return true },
		))

		// WHEN
		_, err = upd.Next()

		// THEN: 更新が成功する
		assert.NoError(t, err)

		// THEN: 全レコードの first_name が "Updated" になっている
		scan := NewTableScan(
			tbl,
			handler.SearchModeStart{},
			func(record Record) bool { return true },
		)
		results, err := fetchAll(scan)
		assert.NoError(t, err)
		assert.Equal(t, 5, len(results))
		for _, record := range results {
			assert.Equal(t, "Updated", string(record[1]))
		}
	})

	t.Run("条件付きで一部のレコードを更新できる", func(t *testing.T) {
		// GIVEN
		tmpdir := t.TempDir()
		InitStorageEngineForTest(t, tmpdir)
		defer handler.Reset()

		var trxId handler.TrxId = 1

		// テーブルアクセスメソッドを取得
		tbl, err := getTableAccessMethod("users")
		assert.NoError(t, err)

		// プライマリキーが "a" のレコードのみ更新
		upd := NewUpdate(trxId, tbl, []SetColumn{
			{Pos: 1, Value: []byte("Jane")},
			{Pos: 2, Value: []byte("Updated")},
		}, NewTableScan(
			tbl,
			handler.SearchModeKey{Key: [][]byte{[]byte("a")}},
			func(record Record) bool {
				return string(record[0]) == "a"
			},
		))

		// WHEN
		_, err = upd.Next()

		// THEN: 更新が成功する
		assert.NoError(t, err)

		// THEN: "a" のレコードが更新され、他は変わらない
		scan := NewTableScan(
			tbl,
			handler.SearchModeStart{},
			func(record Record) bool { return true },
		)
		results, err := fetchAll(scan)
		assert.NoError(t, err)
		assert.Equal(t, 5, len(results))
		assert.Equal(t, Record{[]byte("a"), []byte("Jane"), []byte("Updated")}, results[0])
		assert.Equal(t, Record{[]byte("b"), []byte("Alice"), []byte("Smith")}, results[1])
	})

	t.Run("Filter を組み合わせて特定の条件のレコードを更新できる", func(t *testing.T) {
		// GIVEN
		tmpdir := t.TempDir()
		InitStorageEngineForTest(t, tmpdir)
		defer handler.Reset()

		var trxId handler.TrxId = 1

		// テーブルアクセスメソッドを取得
		tbl, err := getTableAccessMethod("users")
		assert.NoError(t, err)

		// first_name が "Bob" のレコードの last_name を更新
		upd := NewUpdate(trxId, tbl, []SetColumn{
			{Pos: 2, Value: []byte("Williams")},
		}, NewFilter(
			NewTableScan(
				tbl,
				handler.SearchModeStart{},
				func(record Record) bool { return true },
			),
			func(record Record) bool {
				return string(record[1]) == "Bob"
			},
		))

		// WHEN
		_, err = upd.Next()

		// THEN: 更新が成功する
		assert.NoError(t, err)

		// THEN: "Bob" の last_name が "Williams" に更新され、他は変わらない
		scan := NewTableScan(
			tbl,
			handler.SearchModeStart{},
			func(record Record) bool { return true },
		)
		results, err := fetchAll(scan)
		assert.NoError(t, err)
		assert.Equal(t, 5, len(results))
		// "c" = Bob のレコード
		assert.Equal(t, Record{[]byte("c"), []byte("Bob"), []byte("Williams")}, results[2])
		// 他のレコードは変わらない
		assert.Equal(t, []byte("Doe"), results[0][2])
		assert.Equal(t, []byte("Smith"), results[1][2])
		assert.Equal(t, []byte("Davis"), results[3][2])
		assert.Equal(t, []byte("Brown"), results[4][2])
	})

	t.Run("更新後にユニークインデックスも更新されている", func(t *testing.T) {
		// GIVEN
		tmpdir := t.TempDir()
		InitStorageEngineForTest(t, tmpdir)
		defer handler.Reset()

		var trxId handler.TrxId = 1

		// テーブルアクセスメソッドを取得
		tbl, err := getTableAccessMethod("users")
		assert.NoError(t, err)

		// インデックスアクセスメソッドを取得
		idx, err := tbl.GetUniqueIndexByName("last_name")
		assert.NoError(t, err)

		// "a" (last_name = "Doe") の last_name を "Zebra" に更新
		upd := NewUpdate(trxId, tbl, []SetColumn{
			{Pos: 2, Value: []byte("Zebra")},
		}, NewTableScan(
			tbl,
			handler.SearchModeKey{Key: [][]byte{[]byte("a")}},
			func(record Record) bool {
				return string(record[0]) == "a"
			},
		))

		// WHEN
		_, err = upd.Next()

		// THEN: 更新が成功する
		assert.NoError(t, err)

		// THEN: ユニークインデックスで "Zebra" が検索できる
		// SearchIndex の whileCondition にはデコードされたセカンダリキーのみ渡される
		indexScan := NewIndexScan(
			tbl,
			idx,
			handler.SearchModeKey{Key: [][]byte{[]byte("Zebra")}},
			func(record Record) bool {
				return string(record[0]) == "Zebra"
			},
		)
		results, err := fetchAll(indexScan)
		assert.NoError(t, err)
		assert.Equal(t, 1, len(results))
		assert.Equal(t, Record{[]byte("a"), []byte("John"), []byte("Zebra")}, results[0])

		// THEN: ユニークインデックスで旧値 "Doe" が検索できない
		indexScanOld := NewIndexScan(
			tbl,
			idx,
			handler.SearchModeKey{Key: [][]byte{[]byte("Doe")}},
			func(record Record) bool {
				return string(record[0]) == "Doe"
			},
		)
		resultsOld, err := fetchAll(indexScanOld)
		assert.NoError(t, err)
		assert.Equal(t, 0, len(resultsOld))
	})

	t.Run("プライマリキーカラムを更新できる", func(t *testing.T) {
		// GIVEN
		tmpdir := t.TempDir()
		InitStorageEngineForTest(t, tmpdir)
		defer handler.Reset()

		var trxId handler.TrxId = 1

		// テーブルアクセスメソッドを取得
		tbl, err := getTableAccessMethod("users")
		assert.NoError(t, err)

		// プライマリキーを "a" → "z" に変更
		upd := NewUpdate(trxId, tbl, []SetColumn{
			{Pos: 0, Value: []byte("z")},
		}, NewTableScan(
			tbl,
			handler.SearchModeKey{Key: [][]byte{[]byte("a")}},
			func(record Record) bool {
				return string(record[0]) == "a"
			},
		))

		// WHEN
		_, err = upd.Next()

		// THEN: 更新が成功する
		assert.NoError(t, err)

		// THEN: "a" が消え "z" が追加されている
		scan := NewTableScan(
			tbl,
			handler.SearchModeStart{},
			func(record Record) bool { return true },
		)
		results, err := fetchAll(scan)
		assert.NoError(t, err)
		assert.Equal(t, 5, len(results))
		// "a" は存在しない
		assert.Equal(t, []byte("b"), results[0][0])
		// "z" が末尾にある
		assert.Equal(t, []byte("z"), results[4][0])
		assert.Equal(t, []byte("John"), results[4][1])
		assert.Equal(t, []byte("Doe"), results[4][2])
	})

	t.Run("条件に一致するレコードがない場合、何も更新されない", func(t *testing.T) {
		// GIVEN
		tmpdir := t.TempDir()
		InitStorageEngineForTest(t, tmpdir)
		defer handler.Reset()

		var trxId handler.TrxId = 1

		// テーブルアクセスメソッドを取得
		tbl, err := getTableAccessMethod("users")
		assert.NoError(t, err)

		// 存在しない first_name でフィルタ
		upd := NewUpdate(trxId, tbl, []SetColumn{
			{Pos: 2, Value: []byte("Changed")},
		}, NewFilter(
			NewTableScan(
				tbl,
				handler.SearchModeStart{},
				func(record Record) bool { return true },
			),
			func(record Record) bool {
				return string(record[1]) == "NonExistent"
			},
		))

		// WHEN
		_, err = upd.Next()

		// THEN: エラーなしで正常終了
		assert.NoError(t, err)

		// THEN: 全レコードが変更されていない
		scan := NewTableScan(
			tbl,
			handler.SearchModeStart{},
			func(record Record) bool { return true },
		)
		results, err := fetchAll(scan)
		assert.NoError(t, err)
		assert.Equal(t, 5, len(results))
		assert.Equal(t, []byte("Doe"), results[0][2])
		assert.Equal(t, []byte("Smith"), results[1][2])
		assert.Equal(t, []byte("Johnson"), results[2][2])
		assert.Equal(t, []byte("Davis"), results[3][2])
		assert.Equal(t, []byte("Brown"), results[4][2])
	})

	t.Run("空のテーブルに対して更新しても正常に動作する", func(t *testing.T) {
		// GIVEN
		tmpdir := t.TempDir()
		initStorageManagerForTest(t)
		defer handler.Reset()
		_ = tmpdir

		var trxId handler.TrxId = 1
		createTableForTest(t, "empty_table", nil, []handler.ColumnParam{
			{Name: "id", Type: handler.ColumnTypeString},
			{Name: "value", Type: handler.ColumnTypeString},
		})

		// テーブルアクセスメソッドを取得
		tbl, err := getTableAccessMethod("empty_table")
		assert.NoError(t, err)

		upd := NewUpdate(trxId, tbl, []SetColumn{
			{Pos: 1, Value: []byte("new_value")},
		}, NewTableScan(
			tbl,
			handler.SearchModeStart{},
			func(record Record) bool { return true },
		))

		// WHEN
		_, err = upd.Next()

		// THEN
		assert.NoError(t, err)
	})
}
