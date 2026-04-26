package executor

import (
	"testing"

	"github.com/ren-yamanashi/minesql/internal/storage/access"
	"github.com/ren-yamanashi/minesql/internal/storage/handler"
	"github.com/stretchr/testify/assert"
)

func TestIndexScan(t *testing.T) {

	t.Run("正常に IndexScan を作成できる", func(t *testing.T) {
		// GIVEN
		whileCondition := func(record Record) bool {
			return true
		}

		// WHEN
		indexScan := NewIndexScan(
			nil,
			nil,
			access.RecordSearchModeStart{},
			whileCondition,
		)

		// THEN
		assert.NotNil(t, indexScan)
	})

	t.Run("SearchModeStart を使用して Index 検索できる", func(t *testing.T) {
		tmpdir := t.TempDir()
		InitStorageEngineForTest(t, tmpdir)
		defer handler.Reset()

		// GIVEN

		// テーブルアクセスメソッドを取得
		tbl, err := handler.Get().GetTable("users")
		assert.NoError(t, err)

		// インデックスアクセスメソッドを取得
		idx, err := tbl.GetSecondaryIndexByName("last_name")
		assert.NoError(t, err)

		indexScan := NewIndexScan(
			tbl,
			idx,
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

	t.Run("NewIndexScanWithParams で IndexScan を作成できる", func(t *testing.T) {
		// GIVEN
		params := IndexScanParams{
			Table:          nil,
			Index:          nil,
			SearchMode:     access.RecordSearchModeStart{},
			WhileCondition: func(record Record) bool { return true },
			IndexOnly:      true,
			NCols:          3,
			SecColPos:      2,
		}

		// WHEN
		indexScan := NewIndexScanWithParams(params)

		// THEN
		assert.NotNil(t, indexScan)
		assert.True(t, indexScan.indexOnly)
		assert.Equal(t, 3, indexScan.nCols)
		assert.Equal(t, 2, indexScan.secColPos)
	})

	t.Run("index-only scan で PK と UK のみ取得できる", func(t *testing.T) {
		tmpdir := t.TempDir()
		InitStorageEngineForTest(t, tmpdir)
		defer handler.Reset()

		// GIVEN
		tbl, err := handler.Get().GetTable("users")
		assert.NoError(t, err)

		idx, err := tbl.GetSecondaryIndexByName("last_name")
		assert.NoError(t, err)

		// index-only scan: nCols=3 (id, first_name, last_name), secColPos=2 (last_name)
		indexScan := NewIndexScanWithParams(IndexScanParams{
			Table:          tbl,
			Index:          idx,
			SearchMode:     access.RecordSearchModeStart{},
			WhileCondition: func(record Record) bool { return true },
			IndexOnly:      true,
			NCols:          3,
			SecColPos:      2,
		})

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

		// THEN: 5 行、各行 3 カラム (PK + nil + UK)
		assert.Len(t, results, 5)
		for _, r := range results {
			assert.Len(t, r, 3)
			// PK (pos 0) と UK (pos 2) が設定されている
			assert.NotNil(t, r[0], "PK should be set")
			assert.NotNil(t, r[2], "UK should be set")
			// first_name (pos 1) は nil (index-only ではカバーされない)
			assert.Nil(t, r[1], "non-covered column should be nil")
		}

		// セカンダリキー昇順: Brown, Davis, Doe, Johnson, Smith
		assert.Equal(t, "e", string(results[0][0])) // Brown → PK "e"
		assert.Equal(t, "Brown", string(results[0][2]))
		assert.Equal(t, "a", string(results[2][0])) // Doe → PK "a"
		assert.Equal(t, "Doe", string(results[2][2]))
	})

	t.Run("index-only scan で whileCondition が適用される", func(t *testing.T) {
		tmpdir := t.TempDir()
		InitStorageEngineForTest(t, tmpdir)
		defer handler.Reset()

		// GIVEN
		tbl, err := handler.Get().GetTable("users")
		assert.NoError(t, err)

		idx, err := tbl.GetSecondaryIndexByName("last_name")
		assert.NoError(t, err)

		// whileCondition: UK < "J" (Brown, Davis, Doe の 3 件)
		indexScan := NewIndexScanWithParams(IndexScanParams{
			Table:      tbl,
			Index:      idx,
			SearchMode: access.RecordSearchModeStart{},
			WhileCondition: func(record Record) bool {
				return string(record[0]) < "J"
			},
			IndexOnly: true,
			NCols:     3,
			SecColPos: 2,
		})

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

		// THEN: Brown, Davis, Doe の 3 件
		assert.Len(t, results, 3)
		assert.Equal(t, "Brown", string(results[0][2]))
		assert.Equal(t, "Davis", string(results[1][2]))
		assert.Equal(t, "Doe", string(results[2][2]))
	})

	t.Run("SearchModeKey を使用して Index 検索できる", func(t *testing.T) {
		tmpdir := t.TempDir()
		InitStorageEngineForTest(t, tmpdir)
		defer handler.Reset()

		// GIVEN
		// テーブルアクセスメソッドを取得
		tbl, err := handler.Get().GetTable("users")
		assert.NoError(t, err)

		// インデックスアクセスメソッドを取得
		idx, err := tbl.GetSecondaryIndexByName("last_name")
		assert.NoError(t, err)

		indexScan := NewIndexScan(
			tbl,
			idx,
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
