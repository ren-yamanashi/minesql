package executor_test

import (
	"fmt"
	"os"
	"strings"

	"minesql/internal/executor"
	"minesql/internal/storage/handler"
)

// セットアップヘルパー: テーブルを作成し、サンプルデータを挿入する
// テーブルアクセスメソッドとクリーンアップ関数を返す
func setupExample() (*handler.TableHandler, func()) {
	tmpDir, err := os.MkdirTemp("", "executor_example")
	if err != nil {
		panic(err)
	}
	cleanup := func() {
		handler.Reset()
		_ = os.RemoveAll(tmpDir)
	}

	if err = os.Setenv("MINESQL_DATA_DIR", tmpDir); err != nil {
		panic(err)
	}
	if err = os.Setenv("MINESQL_BUFFER_SIZE", "100"); err != nil {
		panic(err)
	}
	handler.Reset()
	handler.Init()

	// テーブルを作成
	ct := executor.NewCreateTable(
		"users",
		1,
		[]handler.IndexParam{
			{Name: "idx_first_name", ColName: "first_name", SecondaryKey: 1},
			{Name: "idx_last_name", ColName: "last_name", SecondaryKey: 2},
		},
		[]handler.ColumnParam{
			{Name: "id", Type: handler.ColumnTypeString},
			{Name: "first_name", Type: handler.ColumnTypeString},
			{Name: "last_name", Type: handler.ColumnTypeString},
		})
	if _, err := ct.Next(); err != nil {
		panic(err)
	}

	// テーブルアクセスメソッドを取得
	e := handler.Get()
	tblMeta, ok := e.Catalog.GetTableMetadataByName("users")
	if !ok {
		panic("table users not found in catalog")
	}
	rawTbl, err := tblMeta.GetTable()
	if err != nil {
		panic(err)
	}
	tbl := handler.NewTableHandler(rawTbl)

	// サンプルデータを挿入
	var trxId handler.TrxId = 1
	ins := executor.NewInsert(
		trxId,
		tbl,
		[]executor.Record{
			{[]byte("z"), []byte("Alice"), []byte("Smith")},
			{[]byte("x"), []byte("Bob"), []byte("Johnson")},
			{[]byte("y"), []byte("Charlie"), []byte("Williams")},
			{[]byte("w"), []byte("Dave"), []byte("Miller")},
			{[]byte("v"), []byte("Eve"), []byte("Brown")},
		})
	if _, err := ins.Next(); err != nil {
		panic(err)
	}

	return tbl, cleanup
}

// レコードを表示するヘルパー
func printExampleRecords(exec executor.Executor) {
	var records []executor.Record
	for {
		record, err := exec.Next()
		if err != nil {
			panic(err)
		}
		if record == nil {
			break
		}
		records = append(records, record)
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

func ExampleTableScan_fullScan() {
	tbl, cleanup := setupExample()
	defer cleanup()

	// フルテーブルスキャン (プライマリキー昇順)
	iter := executor.NewTableScan(
		tbl,
		handler.SearchModeStart{},
		func(record executor.Record) bool { return true },
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

func ExampleTableScan_rangeScan() {
	tbl, cleanup := setupExample()
	defer cleanup()

	// プライマリキーが "w" 以上 "y" 以下の範囲スキャン
	iter := executor.NewTableScan(
		tbl,
		handler.SearchModeKey{Key: [][]byte{[]byte("w")}},
		func(record executor.Record) bool {
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

func ExampleTableScan_constSearch() {
	tbl, cleanup := setupExample()
	defer cleanup()

	// プライマリキーが "y" のレコードを検索
	iter := executor.NewTableScan(
		tbl,
		handler.SearchModeKey{Key: [][]byte{[]byte("y")}},
		func(record executor.Record) bool {
			return string(record[0]) == "y"
		},
	)
	printExampleRecords(iter)

	// Output:
	//   (y, Charlie, Williams)
	//   合計: 1 件
}

func ExampleFilter() {
	tbl, cleanup := setupExample()
	defer cleanup()

	// フルテーブルスキャン + first_name が "Charlie" のレコードのみフィルタ
	iter := executor.NewFilter(
		executor.NewTableScan(
			tbl,
			handler.SearchModeStart{},
			func(record executor.Record) bool { return true },
		),
		func(record executor.Record) bool {
			return string(record[1]) == "Charlie"
		},
	)
	printExampleRecords(iter)

	// Output:
	//   (y, Charlie, Williams)
	//   合計: 1 件
}

func ExampleIndexScan_fullScan() {
	tbl, cleanup := setupExample()
	defer cleanup()

	idxFirstName, err := tbl.GetUniqueIndexByName("idx_first_name")
	if err != nil {
		panic(err)
	}
	idxLastName, err := tbl.GetUniqueIndexByName("idx_last_name")
	if err != nil {
		panic(err)
	}

	// first_name のインデックスで全件スキャン (名前のアルファベット順)
	fmt.Println("=== idx_first_name ===")
	printExampleRecords(executor.NewIndexScan(
		tbl,
		idxFirstName,
		handler.SearchModeStart{},
		func(record executor.Record) bool { return true },
	))

	// last_name のインデックスで全件スキャン (姓のアルファベット順)
	fmt.Println("=== idx_last_name ===")
	printExampleRecords(executor.NewIndexScan(
		tbl,
		idxLastName,
		handler.SearchModeStart{},
		func(record executor.Record) bool { return true },
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

func ExampleIndexScan_rangeScan() {
	tbl, cleanup := setupExample()
	defer cleanup()

	idxLastName, err := tbl.GetUniqueIndexByName("idx_last_name")
	if err != nil {
		panic(err)
	}

	// 姓が "J" 以上 "N" 未満の範囲をインデックスで検索
	iter := executor.NewIndexScan(
		tbl,
		idxLastName,
		handler.SearchModeKey{Key: [][]byte{[]byte("J")}},
		func(secondaryKey executor.Record) bool {
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

func ExampleIndexScan_constSearch() {
	tbl, cleanup := setupExample()
	defer cleanup()

	idxLastName, err := tbl.GetUniqueIndexByName("idx_last_name")
	if err != nil {
		panic(err)
	}

	// 姓が "Miller" のレコードをインデックスで検索
	iter := executor.NewIndexScan(
		tbl,
		idxLastName,
		handler.SearchModeKey{Key: [][]byte{[]byte("Miller")}},
		func(secondaryKey executor.Record) bool {
			return string(secondaryKey[0]) == "Miller"
		},
	)
	printExampleRecords(iter)

	// Output:
	//   (w, Dave, Miller)
	//   合計: 1 件
}

func ExampleProject() {
	tbl, cleanup := setupExample()
	defer cleanup()

	// フルテーブルスキャンから first_name と last_name のみ取得
	iter := executor.NewProject(
		executor.NewTableScan(
			tbl,
			handler.SearchModeStart{},
			func(record executor.Record) bool { return true },
		),
		[]uint16{1, 2},
	)
	printExampleRecords(iter)

	// Output:
	//   (Eve, Brown)
	//   (Dave, Miller)
	//   (Bob, Johnson)
	//   (Charlie, Williams)
	//   (Alice, Smith)
	//   合計: 5 件
}

func ExampleProject_withFilter() {
	tbl, cleanup := setupExample()
	defer cleanup()

	// first_name が "Charlie" のレコードから first_name と last_name を取得
	iter := executor.NewProject(
		executor.NewFilter(
			executor.NewTableScan(
				tbl,
				handler.SearchModeStart{},
				func(record executor.Record) bool { return true },
			),
			func(record executor.Record) bool {
				return string(record[1]) == "Charlie"
			},
		),
		[]uint16{1, 2},
	)
	printExampleRecords(iter)

	// Output:
	//   (Charlie, Williams)
	//   合計: 1 件
}

func ExampleUpdate() {
	tbl, cleanup := setupExample()
	defer cleanup()

	idxLastName, err := tbl.GetUniqueIndexByName("idx_last_name")
	if err != nil {
		panic(err)
	}

	// Alice の last_name を "Anderson" に更新
	var trxId handler.TrxId = 1
	upd := executor.NewUpdate(trxId, tbl, []executor.SetColumn{
		{Pos: 2, Value: []byte("Anderson")},
	}, executor.NewFilter(
		executor.NewTableScan(
			tbl,
			handler.SearchModeStart{},
			func(record executor.Record) bool { return true },
		),
		func(record executor.Record) bool {
			return string(record[1]) == "Alice"
		},
	))
	if _, err := upd.Next(); err != nil {
		panic(err)
	}

	fmt.Println("=== テーブルスキャン ===")
	printExampleRecords(executor.NewTableScan(
		tbl,
		handler.SearchModeStart{},
		func(record executor.Record) bool { return true },
	))

	fmt.Println("=== インデックススキャン (idx_last_name) ===")
	printExampleRecords(executor.NewIndexScan(
		tbl,
		idxLastName,
		handler.SearchModeStart{},
		func(record executor.Record) bool { return true },
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
	tbl, cleanup := setupExample()
	defer cleanup()

	// プライマリキー "v" (Eve) を "a" に変更
	var trxId handler.TrxId = 1
	upd := executor.NewUpdate(trxId, tbl, []executor.SetColumn{
		{Pos: 0, Value: []byte("a")},
	}, executor.NewTableScan(
		tbl,
		handler.SearchModeKey{Key: [][]byte{[]byte("v")}},
		func(record executor.Record) bool {
			return string(record[0]) == "v"
		},
	))
	if _, err := upd.Next(); err != nil {
		panic(err)
	}

	printExampleRecords(executor.NewTableScan(
		tbl,
		handler.SearchModeStart{},
		func(record executor.Record) bool { return true },
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
	tbl, cleanup := setupExample()
	defer cleanup()

	idxLastName, err := tbl.GetUniqueIndexByName("idx_last_name")
	if err != nil {
		panic(err)
	}

	// first_name が "Bob" のレコードを削除
	var trxId handler.TrxId = 1
	del := executor.NewDelete(trxId, tbl, executor.NewFilter(
		executor.NewTableScan(
			tbl,
			handler.SearchModeStart{},
			func(record executor.Record) bool { return true },
		),
		func(record executor.Record) bool {
			return string(record[1]) == "Bob"
		},
	))
	if _, err := del.Next(); err != nil {
		panic(err)
	}

	fmt.Println("=== テーブルスキャン ===")
	printExampleRecords(executor.NewTableScan(
		tbl,
		handler.SearchModeStart{},
		func(record executor.Record) bool { return true },
	))

	fmt.Println("=== インデックススキャン (idx_last_name) ===")
	printExampleRecords(executor.NewIndexScan(
		tbl,
		idxLastName,
		handler.SearchModeStart{},
		func(record executor.Record) bool { return true },
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
