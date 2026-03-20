package executor

import (
	"fmt"
	"os"
	"strings"

	"minesql/internal/access"
	"minesql/internal/catalog"
	"minesql/internal/engine"
)

// セットアップヘルパー: テーブルを作成し、サンプルデータを挿入する
func setupExample() func() {
	tmpDir, err := os.MkdirTemp("", "executor_example")
	if err != nil {
		panic(err)
	}
	cleanup := func() {
		engine.Reset()
		_ = os.RemoveAll(tmpDir)
	}

	if err = os.Setenv("MINESQL_DATA_DIR", tmpDir); err != nil {
		panic(err)
	}
	if err = os.Setenv("MINESQL_BUFFER_SIZE", "100"); err != nil {
		panic(err)
	}
	engine.Reset()
	engine.Init()

	// テーブルを作成
	ct := NewCreateTable(
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
	if err := ct.Execute(); err != nil {
		panic(err)
	}

	// サンプルデータを挿入
	ins := NewInsert(
		"users",
		[]string{"id", "first_name", "last_name"},
		[]Record{
			{[]byte("z"), []byte("Alice"), []byte("Smith")},
			{[]byte("x"), []byte("Bob"), []byte("Johnson")},
			{[]byte("y"), []byte("Charlie"), []byte("Williams")},
			{[]byte("w"), []byte("Dave"), []byte("Miller")},
			{[]byte("v"), []byte("Eve"), []byte("Brown")},
		})
	if err := ins.Execute(); err != nil {
		panic(err)
	}

	return cleanup
}

// レコードを表示するヘルパー
func printExampleRecords(iter RecordIterator) {
	records, err := FetchAll(iter)
	if err != nil {
		panic(err)
	}
	for _, record := range records {
		cols := make([]string, len(record))
		for i, col := range record {
			cols[i] = string(col)
		}
		fmt.Printf("  (%s)\n", strings.Join(cols, ", "))
	}
	fmt.Printf("  合計: %d 件\n", len(records))
}

func ExampleSearchTable_fullScan() {
	cleanup := setupExample()
	defer cleanup()

	// フルテーブルスキャン (プライマリキー昇順)
	iter := NewSearchTable(
		"users",
		access.RecordSearchModeStart{},
		func(record Record) bool { return true },
	)
	printExampleRecords(iter)

	// Output:
	//   (v, Eve, Brown)
	//   (w, Dave, Miller)
	//   (x, Bob, Johnson)
	//   (y, Charlie, Williams)
	//   (z, Alice, Smith)
	//   合計: 5 件
}

func ExampleSearchTable_rangeScan() {
	cleanup := setupExample()
	defer cleanup()

	// プライマリキーが "w" 以上 "y" 以下の範囲スキャン
	iter := NewSearchTable(
		"users",
		access.RecordSearchModeKey{Key: [][]byte{[]byte("w")}},
		func(record Record) bool {
			return string(record[0]) <= "y"
		},
	)
	printExampleRecords(iter)

	// Output:
	//   (w, Dave, Miller)
	//   (x, Bob, Johnson)
	//   (y, Charlie, Williams)
	//   合計: 3 件
}

func ExampleSearchTable_constSearch() {
	cleanup := setupExample()
	defer cleanup()

	// プライマリキーが "y" のレコードを検索
	iter := NewSearchTable(
		"users",
		access.RecordSearchModeKey{Key: [][]byte{[]byte("y")}},
		func(record Record) bool {
			return string(record[0]) == "y"
		},
	)
	printExampleRecords(iter)

	// Output:
	//   (y, Charlie, Williams)
	//   合計: 1 件
}

func ExampleFilter() {
	cleanup := setupExample()
	defer cleanup()

	// フルテーブルスキャン + first_name が "Charlie" のレコードのみフィルタ
	iter := NewFilter(
		NewSearchTable(
			"users",
			access.RecordSearchModeStart{},
			func(record Record) bool { return true },
		),
		func(record Record) bool {
			return string(record[1]) == "Charlie"
		},
	)
	printExampleRecords(iter)

	// Output:
	//   (y, Charlie, Williams)
	//   合計: 1 件
}

func ExampleSearchIndex_fullScan() {
	cleanup := setupExample()
	defer cleanup()

	// first_name のインデックスで全件スキャン (名前のアルファベット順)
	fmt.Println("=== idx_first_name ===")
	printExampleRecords(NewSearchIndex(
		"users",
		"idx_first_name",
		access.RecordSearchModeStart{},
		func(record Record) bool { return true },
	))

	// last_name のインデックスで全件スキャン (姓のアルファベット順)
	fmt.Println("=== idx_last_name ===")
	printExampleRecords(NewSearchIndex(
		"users",
		"idx_last_name",
		access.RecordSearchModeStart{},
		func(record Record) bool { return true },
	))

	// Output:
	// === idx_first_name ===
	//   (z, Alice, Smith)
	//   (x, Bob, Johnson)
	//   (y, Charlie, Williams)
	//   (w, Dave, Miller)
	//   (v, Eve, Brown)
	//   合計: 5 件
	// === idx_last_name ===
	//   (v, Eve, Brown)
	//   (x, Bob, Johnson)
	//   (w, Dave, Miller)
	//   (z, Alice, Smith)
	//   (y, Charlie, Williams)
	//   合計: 5 件
}

func ExampleSearchIndex_rangeScan() {
	cleanup := setupExample()
	defer cleanup()

	// 姓が "J" 以上 "N" 未満の範囲をインデックスで検索
	iter := NewSearchIndex(
		"users",
		"idx_last_name",
		access.RecordSearchModeKey{Key: [][]byte{[]byte("J")}},
		func(secondaryKey Record) bool {
			lastName := string(secondaryKey[0])
			return lastName >= "J" && lastName < "N"
		},
	)
	printExampleRecords(iter)

	// Output:
	//   (x, Bob, Johnson)
	//   (w, Dave, Miller)
	//   合計: 2 件
}

func ExampleSearchIndex_constSearch() {
	cleanup := setupExample()
	defer cleanup()

	// 姓が "Miller" のレコードをインデックスで検索
	iter := NewSearchIndex(
		"users",
		"idx_last_name",
		access.RecordSearchModeKey{Key: [][]byte{[]byte("Miller")}},
		func(secondaryKey Record) bool {
			return string(secondaryKey[0]) == "Miller"
		},
	)
	printExampleRecords(iter)

	// Output:
	//   (w, Dave, Miller)
	//   合計: 1 件
}

func ExampleUpdate() {
	cleanup := setupExample()
	defer cleanup()

	// Alice の last_name を "Anderson" に更新
	upd := NewUpdate("users", []SetColumn{
		{Pos: 2, Value: []byte("Anderson")},
	}, NewFilter(
		NewSearchTable(
			"users",
			access.RecordSearchModeStart{},
			func(record Record) bool { return true },
		),
		func(record Record) bool {
			return string(record[1]) == "Alice"
		},
	))
	if err := upd.Execute(); err != nil {
		panic(err)
	}

	fmt.Println("=== テーブルスキャン ===")
	printExampleRecords(NewSearchTable(
		"users",
		access.RecordSearchModeStart{},
		func(record Record) bool { return true },
	))

	fmt.Println("=== インデックススキャン (idx_last_name) ===")
	printExampleRecords(NewSearchIndex(
		"users",
		"idx_last_name",
		access.RecordSearchModeStart{},
		func(record Record) bool { return true },
	))

	// Output:
	// === テーブルスキャン ===
	//   (v, Eve, Brown)
	//   (w, Dave, Miller)
	//   (x, Bob, Johnson)
	//   (y, Charlie, Williams)
	//   (z, Alice, Anderson)
	//   合計: 5 件
	// === インデックススキャン (idx_last_name) ===
	//   (z, Alice, Anderson)
	//   (v, Eve, Brown)
	//   (x, Bob, Johnson)
	//   (w, Dave, Miller)
	//   (y, Charlie, Williams)
	//   合計: 5 件
}

func ExampleUpdate_primaryKey() {
	cleanup := setupExample()
	defer cleanup()

	// プライマリキー "v" (Eve) を "a" に変更
	upd := NewUpdate("users", []SetColumn{
		{Pos: 0, Value: []byte("a")},
	}, NewSearchTable(
		"users",
		access.RecordSearchModeKey{Key: [][]byte{[]byte("v")}},
		func(record Record) bool {
			return string(record[0]) == "v"
		},
	))
	if err := upd.Execute(); err != nil {
		panic(err)
	}

	printExampleRecords(NewSearchTable(
		"users",
		access.RecordSearchModeStart{},
		func(record Record) bool { return true },
	))

	// Output:
	//   (a, Eve, Brown)
	//   (w, Dave, Miller)
	//   (x, Bob, Johnson)
	//   (y, Charlie, Williams)
	//   (z, Alice, Smith)
	//   合計: 5 件
}

func ExampleDelete() {
	cleanup := setupExample()
	defer cleanup()

	// first_name が "Bob" のレコードを削除
	del := NewDelete("users", NewFilter(
		NewSearchTable(
			"users",
			access.RecordSearchModeStart{},
			func(record Record) bool { return true },
		),
		func(record Record) bool {
			return string(record[1]) == "Bob"
		},
	))
	if err := del.Execute(); err != nil {
		panic(err)
	}

	fmt.Println("=== テーブルスキャン ===")
	printExampleRecords(NewSearchTable(
		"users",
		access.RecordSearchModeStart{},
		func(record Record) bool { return true },
	))

	fmt.Println("=== インデックススキャン (idx_last_name) ===")
	printExampleRecords(NewSearchIndex(
		"users",
		"idx_last_name",
		access.RecordSearchModeStart{},
		func(record Record) bool { return true },
	))

	// Output:
	// === テーブルスキャン ===
	//   (v, Eve, Brown)
	//   (w, Dave, Miller)
	//   (y, Charlie, Williams)
	//   (z, Alice, Smith)
	//   合計: 4 件
	// === インデックススキャン (idx_last_name) ===
	//   (v, Eve, Brown)
	//   (w, Dave, Miller)
	//   (z, Alice, Smith)
	//   (y, Charlie, Williams)
	//   合計: 4 件
}
