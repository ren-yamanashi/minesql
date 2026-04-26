package parser

import (
	"fmt"
	"minesql/internal/executor"
	"minesql/internal/planner"
	"minesql/internal/storage/handler"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestParserIntegration(t *testing.T) {
	t.Run("CREATE TABLE → INSERT → SELECT でレコードを取得できる", func(t *testing.T) {
		// GIVEN
		tmpdir := t.TempDir()
		t.Setenv("MINESQL_DATA_DIR", tmpdir)
		t.Setenv("MINESQL_BUFFER_SIZE", "100")
		handler.Reset()
		handler.Init()
		defer handler.Reset()

		hdl := handler.Get()
		trxId := hdl.BeginTrx()

		executeSql(t, trxId, `
CREATE TABLE users (
	id VARCHAR,
	first_name VARCHAR,
	last_name VARCHAR,
	gender VARCHAR,
	username VARCHAR,
	PRIMARY KEY (id),
	UNIQUE KEY username_UNIQUE (username)
);`)

		executeSql(t, trxId, `
INSERT INTO
	users (id, first_name, last_name, gender, username)
VALUES
	('1', 'John', 'Doe', 'male', 'johndoe'),
	('2', 'John', 'Doe2', 'male', 'johndoe2'),
	('3', 'John', 'Doe3', 'male', 'johndoe3'),
	('4', 'Jane', 'Doe2', 'female', 'janedoe'),
	('5', 'Jonathan', 'Black', 'male', 'jonathanblack'),
	('6', 'Tom', 'Brown', 'male', 'tombrown');`)
		// WHEN
		records := executeSql(t, trxId, `SELECT * FROM users;`)
		assert.NoError(t, hdl.CommitTrx(trxId))

		// THEN
		var sb strings.Builder
		sb.WriteString("=== SELECT 全件 ===\n")
		writeRecords(&sb, records)

		expected := `=== SELECT 全件 ===
  (1, John, Doe, male, johndoe)
  (2, John, Doe2, male, johndoe2)
  (3, John, Doe3, male, johndoe3)
  (4, Jane, Doe2, female, janedoe)
  (5, Jonathan, Black, male, jonathanblack)
  (6, Tom, Brown, male, tombrown)
  合計: 6 件
`
		assert.Equal(t, expected, sb.String())
	})

	t.Run("WHERE 句で等値検索できる", func(t *testing.T) {
		// GIVEN
		tmpdir := t.TempDir()
		t.Setenv("MINESQL_DATA_DIR", tmpdir)
		t.Setenv("MINESQL_BUFFER_SIZE", "100")
		handler.Reset()
		handler.Init()
		defer handler.Reset()

		hdl := handler.Get()
		trxId := hdl.BeginTrx()

		executeSql(t, trxId, `
CREATE TABLE users (
	id VARCHAR, first_name VARCHAR, last_name VARCHAR, gender VARCHAR, username VARCHAR,
	PRIMARY KEY (id), UNIQUE KEY username_UNIQUE (username)
);`)

		executeSql(t, trxId, `
INSERT INTO users (id, first_name, last_name, gender, username) VALUES
	('1', 'John', 'Doe', 'male', 'johndoe'),
	('2', 'John', 'Doe2', 'male', 'johndoe2'),
	('3', 'John', 'Doe3', 'male', 'johndoe3'),
	('4', 'Jane', 'Doe2', 'female', 'janedoe'),
	('5', 'Jonathan', 'Black', 'male', 'jonathanblack'),
	('6', 'Tom', 'Brown', 'male', 'tombrown');`)
		// WHEN
		records := executeSql(t, trxId, `SELECT * FROM users WHERE username = 'janedoe';`)
		assert.NoError(t, hdl.CommitTrx(trxId))

		// THEN
		var sb strings.Builder
		sb.WriteString("=== WHERE 等値検索 ===\n")
		writeRecords(&sb, records)

		expected := `=== WHERE 等値検索 ===
  (4, Jane, Doe2, female, janedoe)
  合計: 1 件
`
		assert.Equal(t, expected, sb.String())
	})

	t.Run("AND と OR の複合条件でフィルタリングできる", func(t *testing.T) {
		// GIVEN
		tmpdir := t.TempDir()
		t.Setenv("MINESQL_DATA_DIR", tmpdir)
		t.Setenv("MINESQL_BUFFER_SIZE", "100")
		handler.Reset()
		handler.Init()
		defer handler.Reset()

		hdl := handler.Get()
		trxId := hdl.BeginTrx()

		executeSql(t, trxId, `
CREATE TABLE users (
	id VARCHAR, first_name VARCHAR, last_name VARCHAR, gender VARCHAR, username VARCHAR,
	PRIMARY KEY (id), UNIQUE KEY username_UNIQUE (username)
);`)

		executeSql(t, trxId, `
INSERT INTO users (id, first_name, last_name, gender, username) VALUES
	('1', 'John', 'Doe', 'male', 'johndoe'),
	('2', 'John', 'Doe2', 'male', 'johndoe2'),
	('3', 'John', 'Doe3', 'male', 'johndoe3'),
	('4', 'Jane', 'Doe2', 'female', 'janedoe'),
	('5', 'Jonathan', 'Black', 'male', 'jonathanblack'),
	('6', 'Tom', 'Brown', 'male', 'tombrown');`)

		// WHEN: (first_name < 'K' AND gender = 'male' AND last_name >= 'Doe') OR first_name = 'Tom'
		records := executeSql(t, trxId, `SELECT * FROM users WHERE first_name < 'K' AND gender = 'male' AND last_name >= 'Doe' OR first_name = 'Tom';`)
		assert.NoError(t, hdl.CommitTrx(trxId))

		// THEN
		var sb strings.Builder
		sb.WriteString("=== AND/OR 複合条件 ===\n")
		writeRecords(&sb, records)

		expected := `=== AND/OR 複合条件 ===
  (1, John, Doe, male, johndoe)
  (2, John, Doe2, male, johndoe2)
  (3, John, Doe3, male, johndoe3)
  (6, Tom, Brown, male, tombrown)
  合計: 4 件
`
		assert.Equal(t, expected, sb.String())
	})

	t.Run("UPDATE でレコードを更新できる", func(t *testing.T) {
		// GIVEN
		tmpdir := t.TempDir()
		t.Setenv("MINESQL_DATA_DIR", tmpdir)
		t.Setenv("MINESQL_BUFFER_SIZE", "100")
		handler.Reset()
		handler.Init()
		defer handler.Reset()

		hdl := handler.Get()
		trxId := hdl.BeginTrx()

		executeSql(t, trxId, `
CREATE TABLE users (
	id VARCHAR, first_name VARCHAR, last_name VARCHAR, gender VARCHAR, username VARCHAR,
	PRIMARY KEY (id), UNIQUE KEY username_UNIQUE (username)
);`)

		executeSql(t, trxId, `
INSERT INTO users (id, first_name, last_name, gender, username) VALUES
	('1', 'John', 'Doe', 'male', 'johndoe'),
	('2', 'John', 'Doe2', 'male', 'johndoe2'),
	('3', 'John', 'Doe3', 'male', 'johndoe3'),
	('4', 'Jane', 'Doe2', 'female', 'janedoe'),
	('5', 'Jonathan', 'Black', 'male', 'jonathanblack'),
	('6', 'Tom', 'Brown', 'male', 'tombrown');`)

		// WHEN
		executeSql(t, trxId, `UPDATE users SET last_name = 'Anderson' WHERE username = 'janedoe';`)
		// THEN: UPDATE 後の全レコードを確認する
		records := executeSql(t, trxId, `SELECT * FROM users;`)
		assert.NoError(t, hdl.CommitTrx(trxId))

		var sb strings.Builder
		sb.WriteString("=== UPDATE 後の全件 ===\n")
		writeRecords(&sb, records)

		expected := `=== UPDATE 後の全件 ===
  (1, John, Doe, male, johndoe)
  (2, John, Doe2, male, johndoe2)
  (3, John, Doe3, male, johndoe3)
  (4, Jane, Anderson, female, janedoe)
  (5, Jonathan, Black, male, jonathanblack)
  (6, Tom, Brown, male, tombrown)
  合計: 6 件
`
		assert.Equal(t, expected, sb.String())
	})

	t.Run("FOREIGN KEY 付きテーブルを作成してレコードを挿入できる", func(t *testing.T) {
		// GIVEN
		tmpdir := t.TempDir()
		t.Setenv("MINESQL_DATA_DIR", tmpdir)
		t.Setenv("MINESQL_BUFFER_SIZE", "100")
		handler.Reset()
		handler.Init()
		defer handler.Reset()

		hdl := handler.Get()
		trxId := hdl.BeginTrx()

		// 親テーブルを作成
		executeSql(t, trxId, `
CREATE TABLE users (
	id VARCHAR,
	name VARCHAR,
	PRIMARY KEY (id)
);`)

		// FK 付き子テーブルを作成
		executeSql(t, trxId, `
CREATE TABLE orders (
	id VARCHAR,
	user_id VARCHAR,
	item VARCHAR,
	PRIMARY KEY (id),
	KEY idx_user_id (user_id),
	FOREIGN KEY fk_user (user_id) REFERENCES users (id)
);`)

		// 親テーブルにレコードを挿入
		executeSql(t, trxId, `
INSERT INTO users (id, name) VALUES
	('1', 'Alice'),
	('2', 'Bob');`)

		// WHEN: 子テーブルにレコードを挿入
		executeSql(t, trxId, `
INSERT INTO orders (id, user_id, item) VALUES
	('100', '1', 'apple'),
	('101', '2', 'banana');`)

		records := executeSql(t, trxId, `SELECT * FROM orders;`)
		assert.NoError(t, hdl.CommitTrx(trxId))

		// THEN
		var sb strings.Builder
		sb.WriteString("=== FK 付きテーブルの全件 ===\n")
		writeRecords(&sb, records)

		expected := `=== FK 付きテーブルの全件 ===
  (100, 1, apple)
  (101, 2, banana)
  合計: 2 件
`
		assert.Equal(t, expected, sb.String())

		// PK 制約がカタログに自動登録されている
		ordersMeta, ok := hdl.Catalog.GetTableMetaByName("orders")
		assert.True(t, ok)
		assert.Equal(t, uint8(1), ordersMeta.PKCount)
	})

	t.Run("DELETE でレコードを削除できる", func(t *testing.T) {
		// GIVEN
		tmpdir := t.TempDir()
		t.Setenv("MINESQL_DATA_DIR", tmpdir)
		t.Setenv("MINESQL_BUFFER_SIZE", "100")
		handler.Reset()
		handler.Init()
		defer handler.Reset()

		hdl := handler.Get()
		trxId := hdl.BeginTrx()

		executeSql(t, trxId, `
CREATE TABLE users (
	id VARCHAR, first_name VARCHAR, last_name VARCHAR, gender VARCHAR, username VARCHAR,
	PRIMARY KEY (id), UNIQUE KEY username_UNIQUE (username)
);`)

		executeSql(t, trxId, `
INSERT INTO users (id, first_name, last_name, gender, username) VALUES
	('1', 'John', 'Doe', 'male', 'johndoe'),
	('2', 'John', 'Doe2', 'male', 'johndoe2'),
	('3', 'John', 'Doe3', 'male', 'johndoe3'),
	('4', 'Jane', 'Doe2', 'female', 'janedoe'),
	('5', 'Jonathan', 'Black', 'male', 'jonathanblack'),
	('6', 'Tom', 'Brown', 'male', 'tombrown');`)

		// WHEN
		executeSql(t, trxId, `DELETE FROM users WHERE first_name = 'John' AND last_name = 'Doe';`)
		// THEN: DELETE 後の全レコードを確認する
		records := executeSql(t, trxId, `SELECT * FROM users;`)
		assert.NoError(t, hdl.CommitTrx(trxId))

		var sb strings.Builder
		sb.WriteString("=== DELETE 後の全件 ===\n")
		writeRecords(&sb, records)

		expected := `=== DELETE 後の全件 ===
  (2, John, Doe2, male, johndoe2)
  (3, John, Doe3, male, johndoe3)
  (4, Jane, Doe2, female, janedoe)
  (5, Jonathan, Black, male, jonathanblack)
  (6, Tom, Brown, male, tombrown)
  合計: 5 件
`
		assert.Equal(t, expected, sb.String())
	})
}

// Executor から全レコードを取得する
func fetchAll(t *testing.T, iter executor.Executor) []executor.Record {
	t.Helper()
	var records []executor.Record
	for {
		record, err := iter.Next()
		assert.NoError(t, err)
		if record == nil {
			return records
		}
		records = append(records, record)
	}
}

// SQL をパース → プラン → 実行して結果を返す
func executeSql(t *testing.T, trxId handler.TrxId, sql string) []executor.Record {
	t.Helper()
	p := NewParser()
	result, err := p.Parse(sql)
	assert.NoError(t, err)

	plan, err := planner.Start(trxId, result)
	assert.NoError(t, err)

	return fetchAll(t, plan.Exec)
}

// レコード一覧を strings.Builder に書き出す
func writeRecords(sb *strings.Builder, records []executor.Record) {
	for _, rec := range records {
		cols := make([]string, len(rec))
		for i, col := range rec {
			cols[i] = string(col)
		}
		fmt.Fprintf(sb, "  (%s)\n", strings.Join(cols, ", "))
	}
	fmt.Fprintf(sb, "  合計: %d 件\n", len(records))
}
