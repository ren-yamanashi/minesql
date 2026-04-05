package executor

import (
	"minesql/internal/storage/handler"
	"minesql/internal/storage/lock"
	"minesql/internal/storage/transaction"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNewTableScan(t *testing.T) {
	t.Run("正常に TableScan を作成できる", func(t *testing.T) {
		// GIVEN
		tmpdir := t.TempDir()
		InitStorageEngineForTest(t, tmpdir)
		defer handler.Reset()

		whileCondition := func(record Record) bool {
			return true
		}

		// テーブルアクセスメソッドを取得
		tbl, err := handler.Get().GetTable("users")
		assert.NoError(t, err)

		// WHEN
		seqScan := NewTableScan(
			0, lock.NewManager(5000), tbl,
			transaction.RecordSearchModeStart{},
			whileCondition,
		)

		// THEN
		assert.NotNil(t, seqScan)
		assert.Equal(t, tbl, seqScan.table)
	})
}

func TestTableScan_Next(t *testing.T) {
	t.Run("SearchModeStart を使用してテーブルを検索できる", func(t *testing.T) {
		// GIVEN
		tmpdir := t.TempDir()
		InitStorageEngineForTest(t, tmpdir)
		defer handler.Reset()

		// テーブルアクセスメソッドを取得
		tbl, err := handler.Get().GetTable("users")
		assert.NoError(t, err)

		seqScan := NewTableScan(
			0, lock.NewManager(5000), tbl,
			transaction.RecordSearchModeStart{},
			func(record Record) bool {
				return string(record[0]) < "c" // プライマリキーが "c" 未満の間、継続
			},
		)

		// WHEN
		var results []Record
		for {
			record, err := seqScan.Next()
			assert.NoError(t, err)
			if record == nil {
				break
			}
			results = append(results, record)
		}

		// THEN
		expected := []Record{
			{[]byte("a"), []byte("John"), []byte("Doe")},
			{[]byte("b"), []byte("Alice"), []byte("Smith")},
		}
		assert.Equal(t, expected, results)
	})

	t.Run("SearchModeKey を使用してテーブルを検索できる", func(t *testing.T) {
		// GIVEN
		tmpdir := t.TempDir()
		InitStorageEngineForTest(t, tmpdir)
		defer handler.Reset()

		// テーブルアクセスメソッドを取得
		tbl, err := handler.Get().GetTable("users")
		assert.NoError(t, err)

		seqScan := NewTableScan(
			0, lock.NewManager(5000), tbl,
			transaction.RecordSearchModeKey{Key: [][]byte{[]byte("b")}},
			func(record Record) bool {
				return string(record[0]) <= "d" // プライマリキーが "d" 以下の間、継続
			},
		)

		// WHEN
		var results []Record
		for {
			record, err := seqScan.Next()
			assert.NoError(t, err)
			if record == nil {
				break
			}
			results = append(results, record)
		}

		// THEN
		expected := []Record{
			{[]byte("b"), []byte("Alice"), []byte("Smith")},
			{[]byte("c"), []byte("Bob"), []byte("Johnson")},
			{[]byte("d"), []byte("Eve"), []byte("Davis")},
		}
		assert.Equal(t, expected, results)
	})
}

// StorageManager を初期化し、サンプルデータを投入する
func InitStorageEngineForTest(t *testing.T, dataDir string) *handler.Handler {
	// グローバル StorageManager を初期化
	// テスト用に一時的に環境変数を設定
	t.Setenv("MINESQL_DATA_DIR", dataDir)
	t.Setenv("MINESQL_BUFFER_SIZE", "10")

	handler.Reset()
	handler.Init()
	hdl := handler.Get()

	// テーブルを作成
	createTable := NewCreateTable("users", 1, []handler.CreateIndexParam{
		{Name: "last_name", ColName: "last_name", UkIdx: 2},
	}, []handler.CreateColumnParam{
		{Name: "id", Type: handler.ColumnTypeString},
		{Name: "first_name", Type: handler.ColumnTypeString},
		{Name: "last_name", Type: handler.ColumnTypeString},
	})
	_, err := createTable.Next()
	assert.NoError(t, err)

	// テーブルアクセスメソッドを取得
	tbl, err := handler.Get().GetTable("users")
	assert.NoError(t, err)

	// 行を挿入
	err = tbl.Insert(hdl.BufferPool, 0, lock.NewManager(5000), [][]byte{[]byte("a"), []byte("John"), []byte("Doe")})
	assert.NoError(t, err)
	err = tbl.Insert(hdl.BufferPool, 0, lock.NewManager(5000), [][]byte{[]byte("b"), []byte("Alice"), []byte("Smith")})
	assert.NoError(t, err)
	err = tbl.Insert(hdl.BufferPool, 0, lock.NewManager(5000), [][]byte{[]byte("c"), []byte("Bob"), []byte("Johnson")})
	assert.NoError(t, err)
	err = tbl.Insert(hdl.BufferPool, 0, lock.NewManager(5000), [][]byte{[]byte("d"), []byte("Eve"), []byte("Davis")})
	assert.NoError(t, err)
	err = tbl.Insert(hdl.BufferPool, 0, lock.NewManager(5000), [][]byte{[]byte("e"), []byte("Charlie"), []byte("Brown")})
	assert.NoError(t, err)

	return hdl
}
