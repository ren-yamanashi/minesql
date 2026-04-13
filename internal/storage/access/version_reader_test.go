package access

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestReadVisibleVersion(t *testing.T) {
	t.Run("現在のバージョンが可視ならそのまま返す", func(t *testing.T) {
		// GIVEN
		bp := initUndoTestDisk(t)
		undoLog, err := NewUndoManager(bp, nil, undoTestFileId)
		assert.NoError(t, err)
		vr := NewVersionReader(undoLog)

		rv := NewReadView(10, nil, 11) // 全てのトランザクションがコミット済み
		current := RecordVersion{
			LastModified: 5,
			RollPtr:      NullUndoPtr,
			DeleteMark:   0,
			Columns:      [][]byte{[]byte("a"), []byte("Alice")},
		}

		// WHEN
		result, found, err := vr.ReadVisibleVersion(rv, current)

		// THEN
		assert.NoError(t, err)
		assert.True(t, found)
		assert.Equal(t, current.Columns, result.Columns)
		assert.Equal(t, byte(0), result.DeleteMark)
	})

	t.Run("現在のバージョンが不可視で undo チェーンから旧バージョンを返す", func(t *testing.T) {
		// GIVEN
		// T1 が INSERT → T2 が UPDATE した行を、T1 のみ可視な ReadView で読む
		bp := initUndoTestDisk(t)
		undoLog, err := NewUndoManager(bp, nil, undoTestFileId)
		assert.NoError(t, err)
		table := createUndoTestTable(t, bp)
		vr := NewVersionReader(undoLog)

		// T1 の INSERT の undo レコード (prevLastModified=0, prevRollPtr=null)
		t1UndoRecord := NewUndoInsertRecord(table, [][]byte{[]byte("a"), []byte("Alice")})
		t1Ptr, err := undoLog.Append(1, UndoInsert, t1UndoRecord)
		assert.NoError(t, err)

		// T2 の UPDATE の undo レコード (prevLastModified=T1, prevRollPtr=T1のundo)
		t2UndoRecord := NewUndoDeleteRecord(
			table,
			[][]byte{[]byte("a"), []byte("Alice")}, // T2 が上書きする前の値
			1, t1Ptr,
		)
		t2Ptr, err := undoLog.Append(2, UndoDelete, t2UndoRecord)
		assert.NoError(t, err)

		// ReadView: T2 は不可視、T1 は可視
		rv := NewReadView(3, []TrxId{2}, 4)

		// B+Tree 上の現在の行: lastModified=T2, rollPtr=T2のundo
		current := RecordVersion{
			LastModified: 2,
			RollPtr:      t2Ptr,
			DeleteMark:   0,
			Columns:      [][]byte{[]byte("a"), []byte("Bob")},
		}

		// WHEN
		result, found, err := vr.ReadVisibleVersion(rv, current)

		// THEN
		assert.NoError(t, err)
		assert.True(t, found)
		assert.Equal(t, []byte("Alice"), result.Columns[1]) // T1 が書いた値
	})

	t.Run("チェーンの末尾まで辿っても可視なバージョンがなければ found=false", func(t *testing.T) {
		// GIVEN
		bp := initUndoTestDisk(t)
		undoLog, err := NewUndoManager(bp, nil, undoTestFileId)
		assert.NoError(t, err)
		table := createUndoTestTable(t, bp)
		vr := NewVersionReader(undoLog)

		// T1 の INSERT の undo レコード
		t1UndoRecord := NewUndoInsertRecord(table, [][]byte{[]byte("a"), []byte("Alice")})
		t1Ptr, err := undoLog.Append(1, UndoInsert, t1UndoRecord)
		assert.NoError(t, err)

		// ReadView: T1 も不可視
		rv := NewReadView(3, []TrxId{1}, 4)

		current := RecordVersion{
			LastModified: 1,
			RollPtr:      t1Ptr,
			DeleteMark:   0,
			Columns:      [][]byte{[]byte("a"), []byte("Alice")},
		}

		// WHEN
		_, found, err := vr.ReadVisibleVersion(rv, current)

		// THEN
		assert.NoError(t, err)
		assert.False(t, found)
	})

	t.Run("rollPtr が null なら即座に found=false", func(t *testing.T) {
		// GIVEN
		bp := initUndoTestDisk(t)
		undoLog, err := NewUndoManager(bp, nil, undoTestFileId)
		assert.NoError(t, err)
		vr := NewVersionReader(undoLog)

		// 不可視な行で rollPtr が null (undo チェーンがない)
		rv := NewReadView(3, []TrxId{1}, 4)
		current := RecordVersion{
			LastModified: 1,
			RollPtr:      NullUndoPtr,
			DeleteMark:   0,
			Columns:      [][]byte{[]byte("a"), []byte("Alice")},
		}

		// WHEN
		_, found, err := vr.ReadVisibleVersion(rv, current)

		// THEN
		assert.NoError(t, err)
		assert.False(t, found)
	})
}
