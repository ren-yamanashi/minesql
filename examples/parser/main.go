package main

import (
	"fmt"
	"minesql/internal/engine"
	"minesql/internal/executor"
	"minesql/internal/parser"
	"minesql/internal/planner"
	"os"
)

// Executor を実行し、レコードを返すヘルパー
func executePlan(exec executor.Executor) ([]executor.Record, error) {
	switch e := exec.(type) {
	case executor.RecordIterator:
		return executor.FetchAll(e)
	case executor.Mutator:
		return nil, e.Execute()
	default:
		return nil, fmt.Errorf("unsupported executor type: %T", exec)
	}
}

func main() {
	dataDir := "examples/parser/data"
	os.RemoveAll(dataDir)
	os.MkdirAll(dataDir, 0750)

	// StorageManager を初期化
	os.Setenv("MINESQL_DATA_DIR", dataDir)
	os.Setenv("MINESQL_BUFFER_SIZE", "100")
	engine.Init()

	createTable()
	insert()
	scan()
	assertEqual()
	filter()
	updateByCondition()
	scanAfterUpdate()
	deleteByCondition()
	scanAfterDelete()
}

func createTable() {
	sql := `
CREATE TABLE users (
	id VARCHAR,
	first_name VARCHAR,
	last_name VARCHAR,
	gender VARCHAR,
	username VARCHAR,
	PRIMARY KEY (id),
	UNIQUE KEY username_UNIQUE (username)
);
`
	p := parser.NewParser()
	result, err := p.Parse(sql)
	if err != nil {
		panic(err)
	}

	exec, err := planner.PlanStart(result)
	if err != nil {
		panic(err)
	}
	records, err := executePlan(exec)
	if err != nil {
		panic(err)
	}
	for _, record := range records {
		for _, col := range record {
			fmt.Print(string(col), " ")
		}
		fmt.Println()
	}
}

func insert() {
	sql := `
INSERT INTO
	users (id, first_name, last_name, gender, username)
VALUES
	('1', 'John', 'Doe', 'male', 'johndoe'),
	('2', 'John', 'Doe2', 'male', 'johndoe2'),
	('3', 'John', 'Doe3', 'male', 'johndoe3'),
	('4', 'Jane', 'Doe2', 'female', 'janedoe'),
	('5', 'Jonathan', 'Black', 'male', 'jonathanblack'),
	('6', 'Tom', 'Brown', 'male', 'tombrown');
	`
	p := parser.NewParser()
	result, err := p.Parse(sql)
	if err != nil {
		panic(err)
	}

	exec, err := planner.PlanStart(result)
	if err != nil {
		panic(err)
	}
	records, err := executePlan(exec)
	if err != nil {
		panic(err)
	}
	for _, record := range records {
		for _, col := range record {
			fmt.Print(string(col), " ")
		}
		fmt.Println()
	}
}

func scan() {
	fmt.Println("=== scan all ===")
	sql := `SELECT * FROM users;`
	p := parser.NewParser()
	result, err := p.Parse(sql)
	if err != nil {
		panic(err)
	}

	exec, err := planner.PlanStart(result)
	if err != nil {
		panic(err)
	}
	records, err := executePlan(exec)
	if err != nil {
		panic(err)
	}
	for _, record := range records {
		for _, col := range record {
			fmt.Print(string(col), " ")
		}
		fmt.Println()
	}
}

func assertEqual() {
	fmt.Println("=== assert equal ===")
	sql := `SELECT * FROM users WHERE username = 'janedoe';`
	p := parser.NewParser()
	result, err := p.Parse(sql)
	if err != nil {
		panic(err)
	}

	exec, err := planner.PlanStart(result)
	if err != nil {
		panic(err)
	}
	records, err := executePlan(exec)
	if err != nil {
		panic(err)
	}
	for _, record := range records {
		for _, col := range record {
			fmt.Print(string(col), " ")
		}
		fmt.Println()
	}
}

func filter() {
	fmt.Println("=== filter (first_name < 'K' AND gender = 'male' AND last_name >= 'Doe') OR first_name = 'Tom' ===")
	sql := `SELECT * FROM users WHERE first_name < 'K' AND gender = 'male' AND last_name >= 'Doe' OR first_name = 'Tom';`
	p := parser.NewParser()
	result, err := p.Parse(sql)
	if err != nil {
		panic(err)
	}

	exec, err := planner.PlanStart(result)
	if err != nil {
		panic(err)
	}
	records, err := executePlan(exec)
	if err != nil {
		panic(err)
	}
	for _, record := range records {
		for _, col := range record {
			fmt.Print(string(col), " ")
		}
		fmt.Println()
	}
}

func updateByCondition() {
	sql := `UPDATE users SET last_name = 'Anderson' WHERE username = 'janedoe';`
	p := parser.NewParser()
	result, err := p.Parse(sql)
	if err != nil {
		panic(err)
	}

	exec, err := planner.PlanStart(result)
	if err != nil {
		panic(err)
	}
	_, err = executePlan(exec)
	if err != nil {
		panic(err)
	}
	fmt.Println("updated.")
}

func scanAfterUpdate() {
	fmt.Println("=== scan after update (UPDATE users SET last_name = 'Anderson' WHERE username = 'janedoe';) ===")
	sql := `SELECT * FROM users;`
	p := parser.NewParser()
	result, err := p.Parse(sql)
	if err != nil {
		panic(err)
	}

	exec, err := planner.PlanStart(result)
	if err != nil {
		panic(err)
	}
	records, err := executePlan(exec)
	if err != nil {
		panic(err)
	}
	for _, record := range records {
		for _, col := range record {
			fmt.Print(string(col), " ")
		}
		fmt.Println()
	}
}

func deleteByCondition() {
	sql := `DELETE FROM users WHERE first_name = 'John' AND last_name = 'Doe';`
	p := parser.NewParser()
	result, err := p.Parse(sql)
	if err != nil {
		panic(err)
	}

	exec, err := planner.PlanStart(result)
	if err != nil {
		panic(err)
	}
	_, err = executePlan(exec)
	if err != nil {
		panic(err)
	}
}

func scanAfterDelete() {
	fmt.Println("=== scan after delete (`DELETE FROM users WHERE first_name = 'John' AND last_name = 'Doe';`) ===")
	sql := `SELECT * FROM users;`
	p := parser.NewParser()
	result, err := p.Parse(sql)
	if err != nil {
		panic(err)
	}

	exec, err := planner.PlanStart(result)
	if err != nil {
		panic(err)
	}
	records, err := executePlan(exec)
	if err != nil {
		panic(err)
	}
	for _, record := range records {
		for _, col := range record {
			fmt.Print(string(col), " ")
		}
		fmt.Println()
	}
}
