package executor

import (
	"minesql/internal/access"
	"minesql/internal/engine"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNewSearchIndex(t *testing.T) {
	t.Run("正常に SearchIndex を作成できる", func(t *testing.T) {
		// GIVEN
		tableName := "users"
		indexName := "last_name"
		whileCondition := func(record Record) bool {
			return true
		}

		// WHEN
		indexScan := NewSearchIndex(
			tableName,
			indexName,
			access.RecordSearchModeStart{},
			whileCondition,
		)

		// THEN
		assert.NotNil(t, indexScan)
	})
}

func TestSearchIndex_Next(t *testing.T) {
	t.Run("SearchModeStart を使用して Index 検索できる", func(t *testing.T) {
		tmpdir := t.TempDir()
		InitStorageEngineForTest(t, tmpdir)
		defer engine.Reset()

		// GIVEN
		indexScan := NewSearchIndex(
			"users",
			"last_name",
			access.RecordSearchModeStart{},
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

	t.Run("SearchModeKey を使用して Index 検索できる", func(t *testing.T) {
		tmpdir := t.TempDir()
		InitStorageEngineForTest(t, tmpdir)
		defer engine.Reset()

		// GIVEN
		indexScan := NewSearchIndex(
			"users",
			"last_name",
			access.RecordSearchModeKey{Key: [][]byte{[]byte("Doe")}},
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
