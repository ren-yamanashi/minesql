package executor

import (
	"minesql/internal/storage"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNewIndexScan(t *testing.T) {
	t.Run("正常に IndexScan を作成できる", func(t *testing.T) {
		// GIVEN
		tableName := "users"
		indexName := "last_name"
		whileCondition := func(record Record) bool {
			return true
		}

		// WHEN
		indexScan := NewIndexScan(
			tableName,
			indexName,
			RecordSearchModeStart{},
			whileCondition,
		)

		// THEN
		assert.NotNil(t, indexScan)
	})
}

func TestIndexScan(t *testing.T) {
	t.Run("インデックスでスキャンできる (SearchModeStart を使用)", func(t *testing.T) {
		tmpdir := t.TempDir()
		InitStorageEngineForTest(t, tmpdir)
		defer storage.ResetStorageEngine()

		// GIVEN
		indexScan := NewIndexScan(
			"users",
			"last_name",
			RecordSearchModeStart{},
			func(record Record) bool {
				return string(record[0]) < "J" // セカンダリキー (姓) が "J" 未満の間、継続
			},
		)

		// WHEN
		var results []Record
		for {
			record, err := indexScan.Next()
			assert.NoError(t, err)
			if record == nil {
				break
			}
			results = append(results, record)
		}

		// THEN
		expected := []Record{
			{[]byte("e"), []byte("Charlie"), []byte("Brown")},
			{[]byte("d"), []byte("Eve"), []byte("Davis")},
			{[]byte("a"), []byte("John"), []byte("Doe")},
		}
		assert.Equal(t, expected, results)
	})

	t.Run("インデックスでスキャンできる (SearchModeKey を使用)", func(t *testing.T) {
		tmpdir := t.TempDir()
		InitStorageEngineForTest(t, tmpdir)
		defer storage.ResetStorageEngine()

		// GIVEN
		indexScan := NewIndexScan(
			"users",
			"last_name",
			RecordSearchModeKey{Key: [][]byte{[]byte("Doe")}},
			func(record Record) bool {
				return string(record[0]) <= "Smith" // セカンダリキー (姓) が "Smith" 以下の間、継続
			},
		)

		// WHEN
		var results []Record
		for {
			record, err := indexScan.Next()
			assert.NoError(t, err)
			if record == nil {
				break
			}
			results = append(results, record)
		}

		// THEN
		expected := []Record{
			{[]byte("a"), []byte("John"), []byte("Doe")},
			{[]byte("c"), []byte("Bob"), []byte("Johnson")},
			{[]byte("b"), []byte("Alice"), []byte("Smith")},
		}
		assert.Equal(t, expected, results)
	})
}
