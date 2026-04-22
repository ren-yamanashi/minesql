package parser_test

import (
	"fmt"
	"os"
	"strings"

	"minesql/internal/executor"
	"minesql/internal/parser"
	"minesql/internal/planner"
	"minesql/internal/storage/handler"
)

// セットアップヘルパー: テーブルを作成し、サンプルデータを挿入する
func setupParserExample(trxId handler.TrxId) func() { //nolint:unparam
	tmpDir, err := os.MkdirTemp("", "parser_example")
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

	runSQL(trxId, `
CREATE TABLE users (
	id VARCHAR,
	first_name VARCHAR,
	last_name VARCHAR,
	gender VARCHAR,
	username VARCHAR,
	PRIMARY KEY (id),
	UNIQUE KEY username_UNIQUE (username)
);`)

	runSQL(trxId, `
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
func runSQL(trxId handler.TrxId, sql string) []executor.Record {
	p := parser.NewParser()
	result, err := p.Parse(sql)
	if err != nil {
		panic(err)
	}

	exec, err := planner.Start(trxId, result)
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
	var trxId handler.TrxId = 1
	cleanup := setupParserExample(trxId)
	defer cleanup()

	records := runSQL(trxId, `SELECT * FROM users;`)
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
	var trxId handler.TrxId = 1
	cleanup := setupParserExample(trxId)
	defer cleanup()

	records := runSQL(trxId, `SELECT * FROM users WHERE username = 'janedoe';`)
	printRecords(records)

	// Output:
	//   (4, Jane, Doe2, female, janedoe)
	//   合計: 1 件
}

func Example_filter() {
	var trxId handler.TrxId = 1
	cleanup := setupParserExample(trxId)
	defer cleanup()
	records := runSQL(trxId, `SELECT * FROM users WHERE first_name < 'K' AND gender = 'male' AND last_name >= 'Doe' OR first_name = 'Tom';`)
	printRecords(records)

	// Output:
	//   (1, John, Doe, male, johndoe)
	//   (2, John, Doe2, male, johndoe2)
	//   (3, John, Doe3, male, johndoe3)
	//   (6, Tom, Brown, male, tombrown)
	//   合計: 4 件
}

func Example_update() {
	var trxId handler.TrxId = 1
	cleanup := setupParserExample(trxId)
	defer cleanup()
	runSQL(trxId, `UPDATE users SET last_name = 'Anderson' WHERE username = 'janedoe';`)
	records := runSQL(trxId, `SELECT * FROM users;`)
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

func Example_join() {
	var trxId handler.TrxId = 1
	cleanup := setupParserExample(trxId)
	defer cleanup()

	// orders テーブルを作成 (user_id に UNIQUE KEY)
	runSQL(trxId, `
CREATE TABLE orders (
	id VARCHAR,
	user_id VARCHAR,
	item VARCHAR,
	PRIMARY KEY (id),
	UNIQUE KEY idx_user_id (user_id)
);`)

	runSQL(trxId, `
INSERT INTO
	orders (id, user_id, item)
VALUES
	('100', '1', 'apple'),
	('101', '3', 'banana');`)

	records := runSQL(trxId, `SELECT * FROM users JOIN orders ON users.id = orders.user_id;`)
	printRecords(records)

	// Output:
	//   (100, 1, apple, 1, John, Doe, male, johndoe)
	//   (101, 3, banana, 3, John, Doe3, male, johndoe3)
	//   合計: 2 件
}

func Example_joinWithWhere() {
	var trxId handler.TrxId = 1
	cleanup := setupParserExample(trxId)
	defer cleanup()

	runSQL(trxId, `
CREATE TABLE orders (
	id VARCHAR,
	user_id VARCHAR,
	item VARCHAR,
	PRIMARY KEY (id),
	UNIQUE KEY idx_user_id (user_id)
);`)

	runSQL(trxId, `
INSERT INTO
	orders (id, user_id, item)
VALUES
	('100', '1', 'apple'),
	('101', '3', 'banana');`)

	records := runSQL(trxId, `SELECT * FROM users JOIN orders ON users.id = orders.user_id WHERE orders.item = 'banana';`)
	printRecords(records)

	// Output:
	//   (101, 3, banana, 3, John, Doe3, male, johndoe3)
	//   合計: 1 件
}

func Example_nonUniqueIndex() {
	var trxId handler.TrxId = 1
	cleanup := setupParserExample(trxId)
	defer cleanup()

	// 非ユニークインデックス付きテーブルを作成
	runSQL(trxId, `
CREATE TABLE products (
	id VARCHAR,
	name VARCHAR,
	category VARCHAR,
	PRIMARY KEY (id),
	KEY idx_category (category)
);`)

	runSQL(trxId, `
INSERT INTO
	products (id, name, category)
VALUES
	('1', 'Apple', 'Fruit'),
	('2', 'Banana', 'Fruit'),
	('3', 'Carrot', 'Veggie');`)

	// 同一カテゴリの複数行が取得できる
	records := runSQL(trxId, `SELECT * FROM products WHERE category = 'Fruit';`)
	printRecords(records)

	// Output:
	//   (1, Apple, Fruit)
	//   (2, Banana, Fruit)
	//   合計: 2 件
}

func Example_foreignKey() {
	var trxId handler.TrxId = 1
	cleanup := setupParserExample(trxId)
	defer cleanup()

	// FK 付き子テーブルを作成
	runSQL(trxId, `
CREATE TABLE orders (
	id VARCHAR,
	user_id VARCHAR,
	item VARCHAR,
	PRIMARY KEY (id),
	KEY idx_user_id (user_id),
	FOREIGN KEY fk_user (user_id) REFERENCES users (id)
);`)

	runSQL(trxId, `
INSERT INTO
	orders (id, user_id, item)
VALUES
	('100', '1', 'apple'),
	('101', '3', 'banana');`)

	records := runSQL(trxId, `SELECT * FROM orders;`)
	printRecords(records)

	// Output:
	//   (100, 1, apple)
	//   (101, 3, banana)
	//   合計: 2 件
}

func Example_delete() {
	var trxId handler.TrxId = 1
	cleanup := setupParserExample(trxId)
	defer cleanup()
	runSQL(trxId, `DELETE FROM users WHERE first_name = 'John' AND last_name = 'Doe';`)
	records := runSQL(trxId, `SELECT * FROM users;`)
	printRecords(records)

	// Output:
	//   (2, John, Doe2, male, johndoe2)
	//   (3, John, Doe3, male, johndoe3)
	//   (4, Jane, Doe2, female, janedoe)
	//   (5, Jonathan, Black, male, jonathanblack)
	//   (6, Tom, Brown, male, tombrown)
	//   合計: 5 件
}
