package executor

import (
	"minesql/internal/storage"
	"minesql/internal/storage/access/catalog"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNewSearchTable(t *testing.T) {
	t.Run("正常に SearchTable を作成できる", func(t *testing.T) {
		// GIVEN
		tmpdir := t.TempDir()
		InitStorageEngineForTest(t, tmpdir)
		defer storage.ResetStorageManager()

		whileCondition := func(record Record) bool {
			return true
		}

		// WHEN
		seqScan := NewSearchTable(
			"users",
			RecordSearchModeStart{},
			whileCondition,
		)

		// THEN
		assert.NotNil(t, seqScan)
		assert.Equal(t, "users", seqScan.tableName)
		assert.Equal(t, RecordSearchModeStart{}, seqScan.searchMode)
	})
}

func TestSearchTable(t *testing.T) {
	t.Run("SearchModeStart を使用してテーブルを検索できる", func(t *testing.T) {
		// GIVEN
		tmpdir := t.TempDir()
		InitStorageEngineForTest(t, tmpdir)
		defer storage.ResetStorageManager()

		seqScan := NewSearchTable(
			"users",
			RecordSearchModeStart{},
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
		defer storage.ResetStorageManager()

		seqScan := NewSearchTable(
			"users",
			RecordSearchModeKey{Key: [][]byte{[]byte("b")}},
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
func InitStorageEngineForTest(t *testing.T, dataDir string) *storage.StorageManager {
	// グローバル StorageManager を初期化
	// テスト用に一時的に環境変数を設定
	t.Setenv("MINESQL_DATA_DIR", dataDir)
	t.Setenv("MINESQL_BUFFER_SIZE", "10")

	storage.ResetStorageManager()
	storage.InitStorageManager()
	sm := storage.GetStorageManager()

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
	err = tbl.Insert(sm.BufferPoolManager, [][]byte{[]byte("a"), []byte("John"), []byte("Doe")})
	err = tbl.Insert(sm.BufferPoolManager, [][]byte{[]byte("b"), []byte("Alice"), []byte("Smith")})
	err = tbl.Insert(sm.BufferPoolManager, [][]byte{[]byte("c"), []byte("Bob"), []byte("Johnson")})
	err = tbl.Insert(sm.BufferPoolManager, [][]byte{[]byte("d"), []byte("Eve"), []byte("Davis")})
	err = tbl.Insert(sm.BufferPoolManager, [][]byte{[]byte("e"), []byte("Charlie"), []byte("Brown")})
	assert.NoError(t, err)

	return sm
}
