package access

import (
	"fmt"
	"minesql/internal/storage/buffer"
	"minesql/internal/storage/file"
	"minesql/internal/storage/page"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
)

const undoTestFileId = page.FileId(99) // テスト用 UNDO FileId

func TestNewUndoManager(t *testing.T) {
	t.Run("空の UndoManager が生成される", func(t *testing.T) {
		// GIVEN
		bp := initUndoTestDisk(t)

		// WHEN
		undoLog, err := NewUndoManager(bp, nil, undoTestFileId)

		// THEN
		assert.NoError(t, err)
		assert.NotNil(t, undoLog)
		assert.Nil(t, undoLog.GetRecords(1))
	})
}

func TestUndoManagerAppend(t *testing.T) {
	t.Run("レコードを追加できる", func(t *testing.T) {
		// GIVEN
		bp := initUndoTestDisk(t)
		undoLog, err := NewUndoManager(bp, nil, undoTestFileId)
		assert.NoError(t, err)
		table := createUndoTestTable(t, bp)

		// WHEN
		ptr1, err := undoLog.Append(1, NewUndoInsertRecord(table, [][]byte{[]byte("a"), []byte("Alice")}))
		assert.NoError(t, err)
		ptr2, err := undoLog.Append(1, NewUndoInsertRecord(table, [][]byte{[]byte("b"), []byte("Bob")}))
		assert.NoError(t, err)

		// THEN
		assert.Equal(t, 2, len(undoLog.GetRecords(1)))
		// 同じページ内で 2 件目のオフセットは 1 件目より大きい
		assert.Equal(t, ptr1.PageNumber, ptr2.PageNumber)
		assert.Greater(t, ptr2.Offset, ptr1.Offset)
	})

	t.Run("異なるトランザクションに独立して追加できる", func(t *testing.T) {
		// GIVEN
		bp := initUndoTestDisk(t)
		undoLog, err := NewUndoManager(bp, nil, undoTestFileId)
		assert.NoError(t, err)
		table := createUndoTestTable(t, bp)

		// WHEN
		ptr1, err := undoLog.Append(1, NewUndoInsertRecord(table, [][]byte{[]byte("a"), []byte("Alice")}))
		assert.NoError(t, err)
		ptr2, err := undoLog.Append(2, NewUndoInsertRecord(table, [][]byte{[]byte("b"), []byte("Bob")}))
		assert.NoError(t, err)
		ptr3, err := undoLog.Append(2, NewUndoInsertRecord(table, [][]byte{[]byte("c"), []byte("Carol")}))
		assert.NoError(t, err)

		// THEN
		assert.Equal(t, 1, len(undoLog.GetRecords(1)))
		assert.Equal(t, 2, len(undoLog.GetRecords(2)))
		// 異なるトランザクションでも同じページに連続して書き込まれる
		assert.Equal(t, ptr1.PageNumber, ptr2.PageNumber)
		assert.Equal(t, ptr2.PageNumber, ptr3.PageNumber)
		assert.Greater(t, ptr2.Offset, ptr1.Offset)
		assert.Greater(t, ptr3.Offset, ptr2.Offset)
	})

	t.Run("ページが満杯になると新しいページに書き込まれる", func(t *testing.T) {
		// GIVEN
		bp := initUndoTestDisk(t)
		undoLog, err := NewUndoManager(bp, nil, undoTestFileId)
		assert.NoError(t, err)
		table := createUndoTestTable(t, bp)

		// WHEN
		// ページが満杯になるまでレコードを追加する
		// 1 レコードあたり undoRecordHeaderSize(19) + テーブル名(2+4) + カラム数(2) + カラム(2+1 + 2+5) = 37 バイト
		// ボディ容量 4084 / 37 ≒ 110 レコードで 1 ページが埋まる
		recordCount := 150
		var firstPtr, lastPtr UndoPtr
		for i := range recordCount {
			col := []byte(fmt.Sprintf("v%04d", i))
			ptr, err := undoLog.Append(1, NewUndoInsertRecord(table, [][]byte{[]byte("a"), col}))
			assert.NoError(t, err)
			if i == 0 {
				firstPtr = ptr
			}
			lastPtr = ptr
		}

		// THEN
		records := undoLog.GetRecords(1)
		assert.Equal(t, recordCount, len(records))
		// ページが切り替わっているため、最初と最後のレコードは異なるページに書き込まれている
		assert.NotEqual(t, firstPtr.PageNumber, lastPtr.PageNumber)
	})
}

func TestReadAt(t *testing.T) {
	t.Run("Append で書き込んだレコードを ReadAt で読み取れる", func(t *testing.T) {
		// GIVEN
		bp := initUndoTestDisk(t)
		undoLog, err := NewUndoManager(bp, nil, undoTestFileId)
		assert.NoError(t, err)
		table := createUndoTestTable(t, bp)

		record := NewUndoInsertRecord(table, [][]byte{[]byte("a"), []byte("Alice")})
		ptr, err := undoLog.Append(1, record)
		assert.NoError(t, err)

		// WHEN
		raw, err := undoLog.ReadAt(ptr)

		// THEN
		assert.NoError(t, err)
		f, err := DeserializeUndoRecord(raw)
		assert.NoError(t, err)
		assert.Equal(t, uint64(1), f.TrxId)
		assert.Equal(t, UndoInsert, f.RecordType)
		assert.Equal(t, table.Name, f.TableName)
		assert.Equal(t, []byte("a"), f.ColumnSets[0][0])
		assert.Equal(t, []byte("Alice"), f.ColumnSets[0][1])
	})

	t.Run("同一ページ内の複数レコードをそれぞれ読み取れる", func(t *testing.T) {
		// GIVEN
		bp := initUndoTestDisk(t)
		undoLog, err := NewUndoManager(bp, nil, undoTestFileId)
		assert.NoError(t, err)
		table := createUndoTestTable(t, bp)

		ptr1, err := undoLog.Append(1, NewUndoInsertRecord(table, [][]byte{[]byte("a"), []byte("Alice")}))
		assert.NoError(t, err)
		ptr2, err := undoLog.Append(1, NewUndoInsertRecord(table, [][]byte{[]byte("b"), []byte("Bob")}))
		assert.NoError(t, err)

		// WHEN
		raw1, err := undoLog.ReadAt(ptr1)
		assert.NoError(t, err)
		raw2, err := undoLog.ReadAt(ptr2)
		assert.NoError(t, err)

		// THEN
		f1, err := DeserializeUndoRecord(raw1)
		assert.NoError(t, err)
		assert.Equal(t, []byte("Alice"), f1.ColumnSets[0][1])

		f2, err := DeserializeUndoRecord(raw2)
		assert.NoError(t, err)
		assert.Equal(t, []byte("Bob"), f2.ColumnSets[0][1])
	})

	t.Run("無効な UndoPtr の場合はエラーを返す", func(t *testing.T) {
		// GIVEN
		bp := initUndoTestDisk(t)
		undoLog, err := NewUndoManager(bp, nil, undoTestFileId)
		assert.NoError(t, err)

		// WHEN: 書き込んでいないオフセットを指定
		_, err = undoLog.ReadAt(UndoPtr{PageNumber: 0, Offset: 9999})

		// THEN
		assert.Error(t, err)
	})
}

func TestGetRecords(t *testing.T) {
	t.Run("存在しないトランザクション ID は nil を返す", func(t *testing.T) {
		// GIVEN
		bp := initUndoTestDisk(t)
		undoLog, err := NewUndoManager(bp, nil, undoTestFileId)
		assert.NoError(t, err)

		// WHEN
		records := undoLog.GetRecords(999)

		// THEN
		assert.Nil(t, records)
	})

	t.Run("追加した順序でレコードが返される", func(t *testing.T) {
		// GIVEN
		bp := initUndoTestDisk(t)
		undoLog, err := NewUndoManager(bp, nil, undoTestFileId)
		assert.NoError(t, err)
		table := createUndoTestTable(t, bp)

		_, err = undoLog.Append(1, NewUndoInsertRecord(table, [][]byte{[]byte("a"), []byte("Alice")}))
		assert.NoError(t, err)
		_, err = undoLog.Append(1, NewUndoDeleteRecord(table, [][]byte{[]byte("b"), []byte("Bob")}, 0, NullUndoPtr))
		assert.NoError(t, err)

		// WHEN
		records := undoLog.GetRecords(1)

		// THEN
		assert.Equal(t, 2, len(records))
		insertRecord, ok := records[0].(UndoInsertRecord)
		assert.True(t, ok)
		assert.Equal(t, []byte("a"), insertRecord.Record[0])

		deleteRecord, ok := records[1].(UndoDeleteRecord)
		assert.True(t, ok)
		assert.Equal(t, []byte("b"), deleteRecord.Record[0])
	})
}

func TestPopLast(t *testing.T) {
	t.Run("最後のレコードが削除される", func(t *testing.T) {
		// GIVEN
		bp := initUndoTestDisk(t)
		undoLog, err := NewUndoManager(bp, nil, undoTestFileId)
		assert.NoError(t, err)
		table := createUndoTestTable(t, bp)

		_, err = undoLog.Append(1, NewUndoInsertRecord(table, [][]byte{[]byte("a"), []byte("Alice")}))
		assert.NoError(t, err)
		_, err = undoLog.Append(1, NewUndoInsertRecord(table, [][]byte{[]byte("b"), []byte("Bob")}))
		assert.NoError(t, err)

		// WHEN
		undoLog.PopLast(1)

		// THEN
		records := undoLog.GetRecords(1)
		assert.Equal(t, 1, len(records))
	})

	t.Run("空のトランザクションに対して PopLast してもパニックしない", func(t *testing.T) {
		// GIVEN
		bp := initUndoTestDisk(t)
		undoLog, err := NewUndoManager(bp, nil, undoTestFileId)
		assert.NoError(t, err)

		// WHEN / THEN
		assert.NotPanics(t, func() {
			undoLog.PopLast(1)
		})
	})

	t.Run("他のトランザクションのレコードに影響しない", func(t *testing.T) {
		// GIVEN
		bp := initUndoTestDisk(t)
		undoLog, err := NewUndoManager(bp, nil, undoTestFileId)
		assert.NoError(t, err)
		table := createUndoTestTable(t, bp)

		_, err = undoLog.Append(1, NewUndoInsertRecord(table, [][]byte{[]byte("a"), []byte("Alice")}))
		assert.NoError(t, err)
		_, err = undoLog.Append(2, NewUndoInsertRecord(table, [][]byte{[]byte("b"), []byte("Bob")}))
		assert.NoError(t, err)
		_, err = undoLog.Append(2, NewUndoInsertRecord(table, [][]byte{[]byte("c"), []byte("Carol")}))
		assert.NoError(t, err)

		// WHEN
		undoLog.PopLast(2)

		// THEN
		assert.Equal(t, 1, len(undoLog.GetRecords(1)))
		assert.Equal(t, 1, len(undoLog.GetRecords(2)))
	})
}

func TestDiscard(t *testing.T) {
	t.Run("指定したトランザクションのログが破棄される", func(t *testing.T) {
		// GIVEN
		bp := initUndoTestDisk(t)
		undoLog, err := NewUndoManager(bp, nil, undoTestFileId)
		assert.NoError(t, err)
		table := createUndoTestTable(t, bp)

		_, err = undoLog.Append(1, NewUndoInsertRecord(table, [][]byte{[]byte("a"), []byte("Alice")}))
		assert.NoError(t, err)
		_, err = undoLog.Append(1, NewUndoInsertRecord(table, [][]byte{[]byte("b"), []byte("Bob")}))
		assert.NoError(t, err)

		// WHEN
		undoLog.Discard(1)

		// THEN
		assert.Nil(t, undoLog.GetRecords(1))
	})

	t.Run("他のトランザクションのログに影響しない", func(t *testing.T) {
		// GIVEN
		bp := initUndoTestDisk(t)
		undoLog, err := NewUndoManager(bp, nil, undoTestFileId)
		assert.NoError(t, err)
		table := createUndoTestTable(t, bp)

		_, err = undoLog.Append(1, NewUndoInsertRecord(table, [][]byte{[]byte("a"), []byte("Alice")}))
		assert.NoError(t, err)
		_, err = undoLog.Append(2, NewUndoInsertRecord(table, [][]byte{[]byte("b"), []byte("Bob")}))
		assert.NoError(t, err)

		// WHEN
		undoLog.Discard(1)

		// THEN
		assert.Nil(t, undoLog.GetRecords(1))
		assert.Equal(t, 1, len(undoLog.GetRecords(2)))
	})

	t.Run("存在しないトランザクション ID を Discard してもパニックしない", func(t *testing.T) {
		// GIVEN
		bp := initUndoTestDisk(t)
		undoLog, err := NewUndoManager(bp, nil, undoTestFileId)
		assert.NoError(t, err)

		// WHEN / THEN
		assert.NotPanics(t, func() {
			undoLog.Discard(999)
		})
	})
}

// initUndoTestDisk はテスト用にバッファプールと UNDO Disk を初期化する
func initUndoTestDisk(t *testing.T) *buffer.BufferPool {
	t.Helper()
	tmpdir := t.TempDir()
	bp := buffer.NewBufferPool(100, nil)

	// UNDO 用 Disk
	undoDm, err := file.NewDisk(undoTestFileId, filepath.Join(tmpdir, "undo.db"))
	assert.NoError(t, err)
	bp.RegisterDisk(undoTestFileId, undoDm)

	// テーブル用 Disk (UndoInsertRecord 等が table を参照するため)
	tableDm, err := file.NewDisk(page.FileId(1), filepath.Join(tmpdir, "test.db"))
	assert.NoError(t, err)
	bp.RegisterDisk(page.FileId(1), tableDm)

	return bp
}

// createUndoTestTable はテスト用のテーブルを作成する (undoLog=nil)
func createUndoTestTable(t *testing.T, bp *buffer.BufferPool) *Table {
	t.Helper()
	metaPageId, err := bp.AllocatePageId(page.FileId(1))
	assert.NoError(t, err)
	table := NewTable("test", metaPageId, 1, nil, nil, nil)
	err = table.Create(bp)
	assert.NoError(t, err)
	return &table
}
