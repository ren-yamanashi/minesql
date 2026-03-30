package parser_test

import (
	"fmt"
	"os"
	"strings"

	"minesql/internal/engine"
	"minesql/internal/executor"
	"minesql/internal/parser"
	"minesql/internal/planner"
	"minesql/internal/storage/transaction"
	"minesql/internal/storage/undo"
)

// セットアップヘルパー: テーブルを作成し、サンプルデータを挿入する
func setupParserExample(undoLog *undo.UndoLog, trxId undo.TrxId) func() {
	tmpDir, err := os.MkdirTemp("", "parser_example")
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

	runSQL(undoLog, trxId, `
CREATE TABLE users (
	id VARCHAR,
	first_name VARCHAR,
	last_name VARCHAR,
	gender VARCHAR,
	username VARCHAR,
	PRIMARY KEY (id),
	UNIQUE KEY username_UNIQUE (username)
);`)

	runSQL(undoLog, trxId, `
INSERT INTO
	users (id, first_name, last_name, gender, username)
VALUES
	('1', 'John', 'Doe', 'male', 'johndoe'),
	('2', 'John', 'Doe2', 'male', 'johndoe2'),
	('3', 'John', 'Doe3', 'male', 'johndoe3'),
	('4', 'Jane', 'Doe2', 'female', 'janedoe'),
	('5', 'Jonathan', 'Black', 'male', 'jonathanblack'),
	('6', 'Tom', 'Brown', 'male', 'tombrown');`)

	return cleanup
}

// SQL をパース → プラン → 実行して結果を返す
func runSQL(undoLog *undo.UndoLog, trxId undo.TrxId, sql string) []executor.Record {
	p := parser.NewParser()
	result, err := p.Parse(sql)
	if err != nil {
		panic(err)
	}

	exec, err := planner.Start(undoLog, trxId, result)
	if err != nil {
		panic(err)
	}

	var records []executor.Record
	for {
		record, err := exec.Next()
		if err != nil {
			panic(err)
		}
		if record == nil {
			return records
		}
		records = append(records, record)
	}
}

// レコードを表示するヘルパー
func printRecords(records []executor.Record) {
	for _, record := range records {
		cols := make([]string, len(record))
		for i, col := range record {
			cols[i] = string(col)
		}
		fmt.Printf("  (%s)\n", strings.Join(cols, ", "))
	}
	fmt.Printf("  合計: %d 件\n", len(records))
}

func Example_scanAll() {
	undoLog := undo.NewUndoLog()
	trxMgr := transaction.NewManager(undoLog)
	trxId := trxMgr.Begin()
	cleanup := setupParserExample(undoLog, trxId)
	defer cleanup()

	records := runSQL(undoLog, trxId, `SELECT * FROM users;`)
	trxMgr.Commit(trxId)
	printRecords(records)

	// Output:
	//   (1, John, Doe, male, johndoe)
	//   (2, John, Doe2, male, johndoe2)
	//   (3, John, Doe3, male, johndoe3)
	//   (4, Jane, Doe2, female, janedoe)
	//   (5, Jonathan, Black, male, jonathanblack)
	//   (6, Tom, Brown, male, tombrown)
	//   合計: 6 件
}

func Example_assertEqual() {
	undoLog := undo.NewUndoLog()
	trxMgr := transaction.NewManager(undoLog)
	trxId := trxMgr.Begin()
	cleanup := setupParserExample(undoLog, trxId)
	defer cleanup()

	records := runSQL(undoLog, trxId, `SELECT * FROM users WHERE username = 'janedoe';`)
	trxMgr.Commit(trxId)
	printRecords(records)

	// Output:
	//   (4, Jane, Doe2, female, janedoe)
	//   合計: 1 件
}

func Example_filter() {
	undoLog := undo.NewUndoLog()
	trxMgr := transaction.NewManager(undoLog)
	trxId := trxMgr.Begin()
	cleanup := setupParserExample(undoLog, trxId)
	defer cleanup()
	records := runSQL(undoLog, trxId, `SELECT * FROM users WHERE first_name < 'K' AND gender = 'male' AND last_name >= 'Doe' OR first_name = 'Tom';`)
	trxMgr.Commit(trxId)
	printRecords(records)

	// Output:
	//   (1, John, Doe, male, johndoe)
	//   (2, John, Doe2, male, johndoe2)
	//   (3, John, Doe3, male, johndoe3)
	//   (6, Tom, Brown, male, tombrown)
	//   合計: 4 件
}

func Example_update() {
	undoLog := undo.NewUndoLog()
	trxMgr := transaction.NewManager(undoLog)
	trxId := trxMgr.Begin()
	cleanup := setupParserExample(undoLog, trxId)
	defer cleanup()
	runSQL(undoLog, trxId, `UPDATE users SET last_name = 'Anderson' WHERE username = 'janedoe';`)
	records := runSQL(undoLog, trxId, `SELECT * FROM users;`)
	trxMgr.Commit(trxId)
	printRecords(records)

	// Output:
	//   (1, John, Doe, male, johndoe)
	//   (2, John, Doe2, male, johndoe2)
	//   (3, John, Doe3, male, johndoe3)
	//   (4, Jane, Anderson, female, janedoe)
	//   (5, Jonathan, Black, male, jonathanblack)
	//   (6, Tom, Brown, male, tombrown)
	//   合計: 6 件
}

func Example_delete() {
	undoLog := undo.NewUndoLog()
	trxMgr := transaction.NewManager(undoLog)
	trxId := trxMgr.Begin()
	cleanup := setupParserExample(undoLog, trxId)
	defer cleanup()
	runSQL(undoLog, trxId, `DELETE FROM users WHERE first_name = 'John' AND last_name = 'Doe';`)
	records := runSQL(undoLog, trxId, `SELECT * FROM users;`)
	trxMgr.Commit(trxId)
	printRecords(records)

	// Output:
	//   (2, John, Doe2, male, johndoe2)
	//   (3, John, Doe3, male, johndoe3)
	//   (4, Jane, Doe2, female, janedoe)
	//   (5, Jonathan, Black, male, jonathanblack)
	//   (6, Tom, Brown, male, tombrown)
	//   合計: 5 件
}
