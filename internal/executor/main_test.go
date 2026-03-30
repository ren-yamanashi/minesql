package executor

import (
	"fmt"
	"minesql/internal/engine"
	"minesql/internal/storage/access"
	"minesql/internal/storage/catalog"
	"minesql/internal/storage/undo"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestExecutorIntegration(t *testing.T) {
	t.Run("フルテーブルスキャンで全レコードを取得できる", func(t *testing.T) {
		// GIVEN
		tbl := setupExecutorTestTable(t)
		defer engine.Reset()

		// WHEN
		records := collectAll(t, NewTableScan(
			tbl,
			access.RecordSearchModeStart{},
			func(record Record) bool { return true },
		))

		// THEN
		var sb strings.Builder
		sb.WriteString("=== テーブルスキャン ===\n")
		writeRecords(&sb, records)

		expected := "" +
			"=== テーブルスキャン ===\n" +
			"  (v, Eve, Brown)\n" +
			"  (w, Dave, Miller)\n" +
			"  (x, Bob, Johnson)\n" +
			"  (y, Charlie, Williams)\n" +
			"  (z, Alice, Smith)\n" +
			"  合計: 5 件\n"
		assert.Equal(t, expected, sb.String())
	})

	t.Run("範囲スキャンで指定範囲のレコードを取得できる", func(t *testing.T) {
		// GIVEN
		tbl := setupExecutorTestTable(t)
		defer engine.Reset()

		// WHEN: プライマリキーが "w" 以上 "y" 以下
		records := collectAll(t, NewTableScan(
			tbl,
			access.RecordSearchModeKey{Key: [][]byte{[]byte("w")}},
			func(record Record) bool {
				return string(record[0]) <= "y"
			},
		))

		// THEN
		var sb strings.Builder
		sb.WriteString("=== 範囲スキャン ===\n")
		writeRecords(&sb, records)

		expected := "" +
			"=== 範囲スキャン ===\n" +
			"  (w, Dave, Miller)\n" +
			"  (x, Bob, Johnson)\n" +
			"  (y, Charlie, Williams)\n" +
			"  合計: 3 件\n"
		assert.Equal(t, expected, sb.String())
	})

	t.Run("定数検索で特定のレコードを取得できる", func(t *testing.T) {
		// GIVEN
		tbl := setupExecutorTestTable(t)
		defer engine.Reset()

		// WHEN
		records := collectAll(t, NewTableScan(
			tbl,
			access.RecordSearchModeKey{Key: [][]byte{[]byte("y")}},
			func(record Record) bool {
				return string(record[0]) == "y"
			},
		))

		// THEN
		var sb strings.Builder
		sb.WriteString("=== 定数検索 ===\n")
		writeRecords(&sb, records)

		expected := "" +
			"=== 定数検索 ===\n" +
			"  (y, Charlie, Williams)\n" +
			"  合計: 1 件\n"
		assert.Equal(t, expected, sb.String())
	})

	t.Run("フィルタースキャンで条件に合うレコードのみ取得できる", func(t *testing.T) {
		// GIVEN
		tbl := setupExecutorTestTable(t)
		defer engine.Reset()

		// WHEN: first_name が "Charlie" のレコード
		records := collectAll(t, NewFilter(
			NewTableScan(
				tbl,
				access.RecordSearchModeStart{},
				func(record Record) bool { return true },
			),
			func(record Record) bool {
				return string(record[1]) == "Charlie"
			},
		))

		// THEN
		var sb strings.Builder
		sb.WriteString("=== フィルタースキャン ===\n")
		writeRecords(&sb, records)

		expected := "" +
			"=== フィルタースキャン ===\n" +
			"  (y, Charlie, Williams)\n" +
			"  合計: 1 件\n"
		assert.Equal(t, expected, sb.String())
	})

	t.Run("インデックススキャンで名前順に全レコードを取得できる", func(t *testing.T) {
		// GIVEN
		tbl := setupExecutorTestTable(t)
		defer engine.Reset()

		idx, err := tbl.GetUniqueIndexByName("idx_first_name")
		assert.NoError(t, err)

		// WHEN
		records := collectAll(t, NewIndexScan(
			tbl,
			idx,
			access.RecordSearchModeStart{},
			func(record Record) bool { return true },
		))

		// THEN
		var sb strings.Builder
		sb.WriteString("=== インデックススキャン (idx_first_name) ===\n")
		writeRecords(&sb, records)

		expected := "" +
			"=== インデックススキャン (idx_first_name) ===\n" +
			"  (z, Alice, Smith)\n" +
			"  (x, Bob, Johnson)\n" +
			"  (y, Charlie, Williams)\n" +
			"  (w, Dave, Miller)\n" +
			"  (v, Eve, Brown)\n" +
			"  合計: 5 件\n"
		assert.Equal(t, expected, sb.String())
	})

	t.Run("インデックス範囲スキャンで姓の範囲を取得できる", func(t *testing.T) {
		// GIVEN
		tbl := setupExecutorTestTable(t)
		defer engine.Reset()

		idx, err := tbl.GetUniqueIndexByName("idx_last_name")
		assert.NoError(t, err)

		// WHEN: 姓が "J" 以上 "N" 未満
		records := collectAll(t, NewIndexScan(
			tbl,
			idx,
			access.RecordSearchModeKey{Key: [][]byte{[]byte("J")}},
			func(secondaryKey Record) bool {
				lastName := string(secondaryKey[0])
				return lastName >= "J" && lastName < "N"
			},
		))

		// THEN
		var sb strings.Builder
		sb.WriteString("=== インデックス範囲スキャン (idx_last_name) ===\n")
		writeRecords(&sb, records)

		expected := "" +
			"=== インデックス範囲スキャン (idx_last_name) ===\n" +
			"  (x, Bob, Johnson)\n" +
			"  (w, Dave, Miller)\n" +
			"  合計: 2 件\n"
		assert.Equal(t, expected, sb.String())
	})

	t.Run("ユニークインデックス検索で特定の姓を取得できる", func(t *testing.T) {
		// GIVEN
		tbl := setupExecutorTestTable(t)
		defer engine.Reset()

		idx, err := tbl.GetUniqueIndexByName("idx_last_name")
		assert.NoError(t, err)

		// WHEN
		records := collectAll(t, NewIndexScan(
			tbl,
			idx,
			access.RecordSearchModeKey{Key: [][]byte{[]byte("Miller")}},
			func(secondaryKey Record) bool {
				return string(secondaryKey[0]) == "Miller"
			},
		))

		// THEN
		var sb strings.Builder
		sb.WriteString("=== ユニークインデックス検索 (idx_last_name) ===\n")
		writeRecords(&sb, records)

		expected := "" +
			"=== ユニークインデックス検索 (idx_last_name) ===\n" +
			"  (w, Dave, Miller)\n" +
			"  合計: 1 件\n"
		assert.Equal(t, expected, sb.String())
	})

	t.Run("UPDATE で値を更新し、インデックスも更新される", func(t *testing.T) {
		// GIVEN
		tbl := setupExecutorTestTable(t)
		defer engine.Reset()

		undoLog := undo.NewUndoLog()
		var trxId undo.TrxId = 1
		idx, err := tbl.GetUniqueIndexByName("idx_last_name")
		assert.NoError(t, err)

		// WHEN: Alice の last_name を Anderson に更新
		upd := NewUpdate(undoLog, trxId, tbl, []SetColumn{
			{Pos: 2, Value: []byte("Anderson")},
		}, NewFilter(
			NewTableScan(
				tbl,
				access.RecordSearchModeStart{},
				func(record Record) bool { return true },
			),
			func(record Record) bool {
				return string(record[1]) == "Alice"
			},
		))
		_, err = upd.Next()
		assert.NoError(t, err)

		// THEN: テーブルスキャンとインデックススキャンの両方で確認
		tableRecords := collectAll(t, NewTableScan(
			tbl,
			access.RecordSearchModeStart{},
			func(record Record) bool { return true },
		))
		idxRecords := collectAll(t, NewIndexScan(
			tbl,
			idx,
			access.RecordSearchModeStart{},
			func(record Record) bool { return true },
		))

		var sb strings.Builder
		sb.WriteString("=== テーブルスキャン ===\n")
		writeRecords(&sb, tableRecords)
		sb.WriteString("=== インデックススキャン (idx_last_name) ===\n")
		writeRecords(&sb, idxRecords)

		expected := "" +
			"=== テーブルスキャン ===\n" +
			"  (v, Eve, Brown)\n" +
			"  (w, Dave, Miller)\n" +
			"  (x, Bob, Johnson)\n" +
			"  (y, Charlie, Williams)\n" +
			"  (z, Alice, Anderson)\n" +
			"  合計: 5 件\n" +
			"=== インデックススキャン (idx_last_name) ===\n" +
			"  (z, Alice, Anderson)\n" +
			"  (v, Eve, Brown)\n" +
			"  (x, Bob, Johnson)\n" +
			"  (w, Dave, Miller)\n" +
			"  (y, Charlie, Williams)\n" +
			"  合計: 5 件\n"
		assert.Equal(t, expected, sb.String())
	})

	t.Run("UPDATE でプライマリキーを変更できる", func(t *testing.T) {
		// GIVEN
		tbl := setupExecutorTestTable(t)
		defer engine.Reset()

		undoLog := undo.NewUndoLog()
		var trxId undo.TrxId = 1

		// WHEN: プライマリキー "v" (Eve) を "a" に変更
		upd := NewUpdate(undoLog, trxId, tbl, []SetColumn{
			{Pos: 0, Value: []byte("a")},
		}, NewTableScan(
			tbl,
			access.RecordSearchModeKey{Key: [][]byte{[]byte("v")}},
			func(record Record) bool {
				return string(record[0]) == "v"
			},
		))
		_, err := upd.Next()
		assert.NoError(t, err)

		// THEN
		records := collectAll(t, NewTableScan(
			tbl,
			access.RecordSearchModeStart{},
			func(record Record) bool { return true },
		))

		var sb strings.Builder
		sb.WriteString("=== テーブルスキャン ===\n")
		writeRecords(&sb, records)

		expected := "" +
			"=== テーブルスキャン ===\n" +
			"  (a, Eve, Brown)\n" +
			"  (w, Dave, Miller)\n" +
			"  (x, Bob, Johnson)\n" +
			"  (y, Charlie, Williams)\n" +
			"  (z, Alice, Smith)\n" +
			"  合計: 5 件\n"
		assert.Equal(t, expected, sb.String())
	})

	t.Run("Project で特定のカラムだけ取得できる", func(t *testing.T) {
		// GIVEN
		tbl := setupExecutorTestTable(t)
		defer engine.Reset()

		// WHEN: first_name (pos=1) と last_name (pos=2) のみ取得
		records := collectAll(t, NewProject(
			NewTableScan(
				tbl,
				access.RecordSearchModeStart{},
				func(record Record) bool { return true },
			),
			[]uint16{1, 2},
		))

		// THEN
		var sb strings.Builder
		sb.WriteString("=== 射影 (first_name, last_name) ===\n")
		writeRecords(&sb, records)

		expected := "" +
			"=== 射影 (first_name, last_name) ===\n" +
			"  (Eve, Brown)\n" +
			"  (Dave, Miller)\n" +
			"  (Bob, Johnson)\n" +
			"  (Charlie, Williams)\n" +
			"  (Alice, Smith)\n" +
			"  合計: 5 件\n"
		assert.Equal(t, expected, sb.String())
	})

	t.Run("Filter + Project で条件に合うレコードの特定カラムを取得できる", func(t *testing.T) {
		// GIVEN
		tbl := setupExecutorTestTable(t)
		defer engine.Reset()

		// WHEN: first_name が "Charlie" のレコードから first_name と last_name を取得
		records := collectAll(t, NewProject(
			NewFilter(
				NewTableScan(
					tbl,
					access.RecordSearchModeStart{},
					func(record Record) bool { return true },
				),
				func(record Record) bool {
					return string(record[1]) == "Charlie"
				},
			),
			[]uint16{1, 2},
		))

		// THEN
		var sb strings.Builder
		sb.WriteString("=== Filter + Project ===\n")
		writeRecords(&sb, records)

		expected := "" +
			"=== Filter + Project ===\n" +
			"  (Charlie, Williams)\n" +
			"  合計: 1 件\n"
		assert.Equal(t, expected, sb.String())
	})

	t.Run("DELETE でレコードを削除し、インデックスからも削除される", func(t *testing.T) {
		// GIVEN
		tbl := setupExecutorTestTable(t)
		defer engine.Reset()

		undoLog := undo.NewUndoLog()
		var trxId undo.TrxId = 1
		idx, err := tbl.GetUniqueIndexByName("idx_last_name")
		assert.NoError(t, err)

		// WHEN: Bob を削除
		del := NewDelete(undoLog, trxId, tbl, NewFilter(
			NewTableScan(
				tbl,
				access.RecordSearchModeStart{},
				func(record Record) bool { return true },
			),
			func(record Record) bool {
				return string(record[1]) == "Bob"
			},
		))
		_, err = del.Next()
		assert.NoError(t, err)

		// THEN: テーブルスキャンとインデックススキャンの両方で確認
		tableRecords := collectAll(t, NewTableScan(
			tbl,
			access.RecordSearchModeStart{},
			func(record Record) bool { return true },
		))
		idxRecords := collectAll(t, NewIndexScan(
			tbl,
			idx,
			access.RecordSearchModeStart{},
			func(record Record) bool { return true },
		))

		var sb strings.Builder
		sb.WriteString("=== テーブルスキャン ===\n")
		writeRecords(&sb, tableRecords)
		sb.WriteString("=== インデックススキャン (idx_last_name) ===\n")
		writeRecords(&sb, idxRecords)

		expected := "" +
			"=== テーブルスキャン ===\n" +
			"  (v, Eve, Brown)\n" +
			"  (w, Dave, Miller)\n" +
			"  (y, Charlie, Williams)\n" +
			"  (z, Alice, Smith)\n" +
			"  合計: 4 件\n" +
			"=== インデックススキャン (idx_last_name) ===\n" +
			"  (v, Eve, Brown)\n" +
			"  (w, Dave, Miller)\n" +
			"  (z, Alice, Smith)\n" +
			"  (y, Charlie, Williams)\n" +
			"  合計: 4 件\n"
		assert.Equal(t, expected, sb.String())
	})
}

// 5 人のユーザーを持つテーブルを作成し、テーブルアクセスメソッドを返す
func setupExecutorTestTable(t *testing.T) *access.TableAccessMethod {
	t.Helper()

	tmpdir := t.TempDir()
	t.Setenv("MINESQL_DATA_DIR", tmpdir)
	t.Setenv("MINESQL_BUFFER_SIZE", "100")
	engine.Reset()
	engine.Init()

	createTable := NewCreateTable(
		"users",
		1,
		[]*IndexParam{
			{Name: "idx_first_name", ColName: "first_name", SecondaryKey: 1},
			{Name: "idx_last_name", ColName: "last_name", SecondaryKey: 2},
		},
		[]*ColumnParam{
			{Name: "id", Type: catalog.ColumnTypeString},
			{Name: "first_name", Type: catalog.ColumnTypeString},
			{Name: "last_name", Type: catalog.ColumnTypeString},
		})
	_, err := createTable.Next()
	assert.NoError(t, err)

	tbl, err := getTableAccessMethod("users")
	assert.NoError(t, err)

	undoLog := undo.NewUndoLog()
	var trxId undo.TrxId = 1
	insert := NewInsert(
		undoLog,
		trxId,
		tbl,
		[]Record{
			{[]byte("z"), []byte("Alice"), []byte("Smith")},
			{[]byte("x"), []byte("Bob"), []byte("Johnson")},
			{[]byte("y"), []byte("Charlie"), []byte("Williams")},
			{[]byte("w"), []byte("Dave"), []byte("Miller")},
			{[]byte("v"), []byte("Eve"), []byte("Brown")},
		})
	_, err = insert.Next()
	assert.NoError(t, err)

	return tbl
}

// Executor から全レコードを取得する
func collectAll(t *testing.T, executor Executor) []Record {
	t.Helper()
	records, err := fetchAll(executor)
	assert.NoError(t, err)
	return records
}

func fetchAll(iter Executor) ([]Record, error) {
	var results []Record
	for {
		record, err := iter.Next()
		if err != nil {
			return nil, err
		}
		if record == nil {
			break
		}
		results = append(results, record)
	}
	return results, nil
}

// レコード一覧を strings.Builder に書き込む
func writeRecords(sb *strings.Builder, records []Record) {
	for _, r := range records {
		vals := make([]string, len(r))
		for i, col := range r {
			vals[i] = string(col)
		}
		fmt.Fprintf(sb, "  (%s)\n", strings.Join(vals, ", "))
	}
	fmt.Fprintf(sb, "  合計: %d 件\n", len(records))
}

func getTableAccessMethod(tableName string) (*access.TableAccessMethod, error) {
	e := engine.Get()
	tblMeta, ok := e.Catalog.GetTableMetadataByName(tableName)
	if !ok {
		return nil, fmt.Errorf("table %s not found in catalog", tableName)
	}

	tbl, err := tblMeta.GetTable()
	if err != nil {
		return nil, err
	}
	return tbl, nil
}
