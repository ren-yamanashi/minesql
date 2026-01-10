package executor

import (
	"minesql/internal/storage/access/table"
	"minesql/internal/storage/bufferpool"
	"minesql/internal/storage/disk"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSequentialScan(t *testing.T) {
	t.Run("テーブルをシーケンシャルスキャンできる", func(t *testing.T) {
		tmpdir := t.TempDir()
		path := filepath.Join(tmpdir, "test.db")
		dm, _ := disk.NewDiskManager(path)
		bpm := bufferpool.NewBufferPoolManager(dm, 10)
		table := InitTable(t, bpm)

		// GIVEN
		seqScan, err := NewExecSequentialScan(
			bpm,
			table,
			func(record Record) bool {
				return string(record[0]) < "c" // プライマリキーが "c" 未満の間、継続
			},
		)
		assert.NoError(t, err)

		// WHEN
		var results []Record
		for {
			record, err := seqScan.Next(bpm)
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
}

func InitTable(t *testing.T, bpm *bufferpool.BufferPoolManager) table.Table {
	table := table.Table{
		MetaPageId:      disk.PageId(0),
		PrimaryKeyCount: 1,
	}

	// テーブルを作成
	err := table.Create(bpm)
	assert.NoError(t, err)

	// 行を挿入
	err = table.Insert(bpm, [][]byte{[]byte("a"), []byte("John"), []byte("Doe")})
	err = table.Insert(bpm, [][]byte{[]byte("b"), []byte("Alice"), []byte("Smith")})
	err = table.Insert(bpm, [][]byte{[]byte("c"), []byte("Bob"), []byte("Johnson")})
	err = table.Insert(bpm, [][]byte{[]byte("d"), []byte("Eve"), []byte("Davis")})
	assert.NoError(t, err)

	return table
}
