package executor

import (
	"minesql/internal/storage/access"
	"minesql/internal/storage/handler"
	"minesql/internal/storage/lock"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNewFilter(t *testing.T) {
	t.Run("正常に Filter を作成できる", func(t *testing.T) {
		// GIVEN
		dummyInnerExecutor := NewTableScan(
			0, lock.NewManager(5000), nil,
			access.RecordSearchModeStart{},
			func(record Record) bool { return true },
		)
		condition := func(record Record) bool {
			return string(record[0]) == "a"
		}

		// WHEN
		filter := NewFilter(dummyInnerExecutor, condition)

		// THEN
		assert.NotNil(t, filter)
	})
}

func TestNext(t *testing.T) {
	t.Run("条件を満たすレコードを正しく返す", func(t *testing.T) {
		tmpdir := t.TempDir()
		InitStorageEngineForTest(t, tmpdir)
		defer handler.Reset()

		// GIVEN
		// テーブルアクセスメソッドを取得
		tbl, err := handler.Get().GetTable("users")
		assert.NoError(t, err)

		seqScan := NewTableScan(
			0, lock.NewManager(5000), tbl,
			access.RecordSearchModeStart{},
			func(record Record) bool { return true },
		)
		filter := NewFilter(seqScan, func(record Record) bool {
			return string(record[0]) == "b"
		})

		// WHEN
		record, err := filter.Next()

		// THEN
		assert.NoError(t, err)
		assert.Equal(t, "b", string(record[0]))
		assert.Equal(t, "Alice", string(record[1]))
		assert.Equal(t, "Smith", string(record[2]))
	})

	t.Run("条件を満たすレコードがない場合、nil を返す", func(t *testing.T) {
		tmpdir := t.TempDir()
		InitStorageEngineForTest(t, tmpdir)
		defer handler.Reset()

		// GIVEN
		// テーブルアクセスメソッドを取得
		tbl, err := handler.Get().GetTable("users")
		assert.NoError(t, err)
		seqScan := NewTableScan(
			0, lock.NewManager(5000), tbl,
			access.RecordSearchModeStart{},
			func(record Record) bool { return true },
		)
		filter := NewFilter(seqScan, func(record Record) bool {
			return string(record[0]) == "z"
		})

		// WHEN
		record, err := filter.Next()

		// THEN
		assert.NoError(t, err)
		assert.Nil(t, record)
	})
}
