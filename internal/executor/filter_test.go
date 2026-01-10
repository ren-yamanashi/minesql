package executor

import (
	"minesql/internal/storage/access/btree"
	"minesql/internal/storage/bufferpool"
	"minesql/internal/storage/disk"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNewFilter(t *testing.T) {
	t.Run("正常に Filter を作成できる", func(t *testing.T) {
		// GIVEN
		dummyInnerExecutor := InitSequentialScanExecutor(t)
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
		bpm := bufferpool.NewBufferPoolManager(nil, 10)
		seqScan := InitSequentialScanExecutor(t)

		// GIVEN
		filter := NewFilter(seqScan, func(record Record) bool {
			return string(record[0]) == "b"
		})

		// WHEN
		record, err := filter.Next(bpm)

		// THEN
		assert.NoError(t, err)
		assert.Equal(t, "b", string(record[0]))
		assert.Equal(t, "Alice", string(record[1]))
		assert.Equal(t, "Smith", string(record[2]))
	})

	t.Run("条件を満たすレコードがない場合、nil を返す", func(t *testing.T) {
		bpm := bufferpool.NewBufferPoolManager(nil, 10)
		seqScan := InitSequentialScanExecutor(t)

		// GIVEN
		filter := NewFilter(seqScan, func(record Record) bool {
			return string(record[0]) == "z"
		})

		// WHEN
		record, err := filter.Next(bpm)

		// THEN
		assert.NoError(t, err)
		assert.Nil(t, record)
	})
}

// すべてのレコードをスキャンする SequentialScan Executor を初期化
func InitSequentialScanExecutor(t *testing.T) *SequentialScan {
	tmpdir := t.TempDir()
	path := filepath.Join(tmpdir, "test.db")
	dm, _ := disk.NewDiskManager(path)
	bpm := bufferpool.NewBufferPoolManager(dm, 10)
	table := InitTable(t, bpm) // @see sequential_scan_test.go
	btr := btree.NewBTree(table.MetaPageId)
	tableIterator, _ := btr.Search(bpm, btree.SearchModeStart{})
	seqScan := NewSequentialScan(
		tableIterator,
		func(record Record) bool {
			return true
		},
	)
	return seqScan
}
