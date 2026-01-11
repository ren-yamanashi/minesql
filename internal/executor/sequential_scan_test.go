package executor

import (
	"minesql/internal/storage"
	"minesql/internal/storage/access/table"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNewSequentialScan(t *testing.T) {
	t.Run("正常に SequentialScan を作成できる", func(t *testing.T) {
		// GIVEN
		tmpdir := t.TempDir()
		InitStorageEngineForTest(t, tmpdir)
		defer storage.ResetStorageEngine()

		whileCondition := func(record Record) bool {
			return true
		}

		// WHEN
		seqScan := NewSequentialScan(
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

func TestSequentialScan(t *testing.T) {
	t.Run("テーブルをシーケンシャルスキャンできる (SearchModeStart を使用)", func(t *testing.T) {
		// GIVEN
		tmpdir := t.TempDir()
		InitStorageEngineForTest(t, tmpdir)
		defer storage.ResetStorageEngine()

		seqScan := NewSequentialScan(
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

	t.Run("テーブルをシーケンシャルスキャンできる (SearchModeKey を使用)", func(t *testing.T) {
		// GIVEN
		tmpdir := t.TempDir()
		InitStorageEngineForTest(t, tmpdir)
		defer storage.ResetStorageEngine()

		seqScan := NewSequentialScan(
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

// StorageEngine を初期化し、サンプルデータを投入する
func InitStorageEngineForTest(t *testing.T, dataDir string) *storage.StorageEngine {
	// グローバル StorageEngine を初期化
	// テスト用に一時的に環境変数を設定
	t.Setenv("MINESQL_DATA_DIR", dataDir)
	t.Setenv("MINESQL_BUFFER_SIZE", "10")

	storage.ResetStorageEngine()
	storage.InitStorageEngine()
	engine := storage.GetStorageEngine()

	// テーブルを作成
	uniqueIndexes := table.NewUniqueIndex("last_name", 2)
	createTable := NewCreateTable()
	err := createTable.Execute("users", 1, []*table.UniqueIndex{uniqueIndexes})
	assert.NoError(t, err)

	tbl, err := engine.GetTable("users")
	assert.NoError(t, err)

	bpm := engine.GetBufferPoolManager()

	// 行を挿入
	err = tbl.Insert(bpm, [][]byte{[]byte("a"), []byte("John"), []byte("Doe")})
	err = tbl.Insert(bpm, [][]byte{[]byte("b"), []byte("Alice"), []byte("Smith")})
	err = tbl.Insert(bpm, [][]byte{[]byte("c"), []byte("Bob"), []byte("Johnson")})
	err = tbl.Insert(bpm, [][]byte{[]byte("d"), []byte("Eve"), []byte("Davis")})
	err = tbl.Insert(bpm, [][]byte{[]byte("e"), []byte("Charlie"), []byte("Brown")})
	assert.NoError(t, err)

	return engine
}
