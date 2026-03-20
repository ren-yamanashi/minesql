package executor

import (
	"minesql/internal/access"
	"minesql/internal/catalog"
	"minesql/internal/engine"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNewSearchTable(t *testing.T) {
	t.Run("正常に SearchTable を作成できる", func(t *testing.T) {
		// GIVEN
		tmpdir := t.TempDir()
		InitStorageEngineForTest(t, tmpdir)
		defer engine.Reset()

		whileCondition := func(record Record) bool {
			return true
		}

		// WHEN
		seqScan := NewSearchTable(
			"users",
			access.RecordSearchModeStart{},
			whileCondition,
		)

		// THEN
		assert.NotNil(t, seqScan)
		assert.Equal(t, "users", seqScan.tableName)
	})
}

func TestSearchTable_Next(t *testing.T) {
	t.Run("SearchModeStart を使用してテーブルを検索できる", func(t *testing.T) {
		// GIVEN
		tmpdir := t.TempDir()
		InitStorageEngineForTest(t, tmpdir)
		defer engine.Reset()

		seqScan := NewSearchTable(
			"users",
			access.RecordSearchModeStart{},
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
		defer engine.Reset()

		seqScan := NewSearchTable(
			"users",
			access.RecordSearchModeKey{Key: [][]byte{[]byte("b")}},
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
func InitStorageEngineForTest(t *testing.T, dataDir string) *engine.Engine {
	// グローバル StorageManager を初期化
	// テスト用に一時的に環境変数を設定
	t.Setenv("MINESQL_DATA_DIR", dataDir)
	t.Setenv("MINESQL_BUFFER_SIZE", "10")

	engine.Reset()
	engine.Init()
	sm := engine.Get()

	// テーブルを作成
	createTable := NewCreateTable("users", 1, []*IndexParam{
		{Name: "last_name", ColName: "last_name", SecondaryKey: 2},
	}, []*ColumnParam{
		{Name: "id", Type: catalog.ColumnTypeString},
		{Name: "first_name", Type: catalog.ColumnTypeString},
		{Name: "last_name", Type: catalog.ColumnTypeString},
	})
	_, err := createTable.Next()
	assert.NoError(t, err)

	tblMeta, err := sm.Catalog.GetTableMetadataByName("users")
	assert.NoError(t, err)
	assert.NotNil(t, tblMeta)

	tbl, err := tblMeta.GetTable()
	assert.NoError(t, err)

	// 行を挿入
	err = tbl.Insert(sm.BufferPool, [][]byte{[]byte("a"), []byte("John"), []byte("Doe")})
	assert.NoError(t, err)
	err = tbl.Insert(sm.BufferPool, [][]byte{[]byte("b"), []byte("Alice"), []byte("Smith")})
	assert.NoError(t, err)
	err = tbl.Insert(sm.BufferPool, [][]byte{[]byte("c"), []byte("Bob"), []byte("Johnson")})
	assert.NoError(t, err)
	err = tbl.Insert(sm.BufferPool, [][]byte{[]byte("d"), []byte("Eve"), []byte("Davis")})
	assert.NoError(t, err)
	err = tbl.Insert(sm.BufferPool, [][]byte{[]byte("e"), []byte("Charlie"), []byte("Brown")})
	assert.NoError(t, err)

	return sm
}
