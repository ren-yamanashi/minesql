package access

import (
	"minesql/internal/storage/buffer"
	"minesql/internal/storage/lock"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestPurgeDeleteMarked(t *testing.T) {
	t.Run("delete-marked かつ purgeLimit より古いレコードが物理削除される", func(t *testing.T) {
		// GIVEN
		bp := initUndoTestDisk(t)
		table := createUndoTestTable(t, bp)
		lockMgr := lock.NewManager(5000)

		err := table.Insert(bp, 1, lockMgr, [][]byte{[]byte("a"), []byte("Alice")})
		assert.NoError(t, err)
		err = table.Insert(bp, 1, lockMgr, [][]byte{[]byte("b"), []byte("Bob")})
		assert.NoError(t, err)
		lockMgr.ReleaseAll(1)

		err = table.SoftDelete(bp, 2, lockMgr, [][]byte{[]byte("a"), []byte("Alice")})
		assert.NoError(t, err)
		lockMgr.ReleaseAll(2)

		// WHEN: purgeLimit=3 でパージ実行
		pt := newTestPurgeThread(bp, lockMgr, table)
		err = pt.purgeDeleteMarked(table, 3)
		assert.NoError(t, err)

		// THEN: "a" が物理削除され、"b" だけが残る
		records := collectPurgeTestRecords(t, bp, table)
		assert.Equal(t, 1, len(records))
		assert.Equal(t, "b", string(records[0][0]))
	})

	t.Run("purgeLimit 以上の lastModified を持つ delete-marked レコードは物理削除されない", func(t *testing.T) {
		// GIVEN
		bp := initUndoTestDisk(t)
		table := createUndoTestTable(t, bp)
		lockMgr := lock.NewManager(5000)

		err := table.Insert(bp, 1, lockMgr, [][]byte{[]byte("a"), []byte("Alice")})
		assert.NoError(t, err)
		lockMgr.ReleaseAll(1)

		err = table.SoftDelete(bp, 5, lockMgr, [][]byte{[]byte("a"), []byte("Alice")})
		assert.NoError(t, err)
		lockMgr.ReleaseAll(5)

		// WHEN: purgeLimit=5 でパージ実行 (lastModified=5 は purgeLimit 以上なのでパージ不可)
		pt := newTestPurgeThread(bp, lockMgr, table)
		err = pt.purgeDeleteMarked(table, 5)
		assert.NoError(t, err)

		// THEN: B+Tree 上にはまだ存在する (物理削除されていない)
		targets, err := pt.collectPurgeTargets(table, 100)
		assert.NoError(t, err)
		assert.Equal(t, 1, len(targets))
	})

	t.Run("delete-marked でないレコードは物理削除されない", func(t *testing.T) {
		// GIVEN
		bp := initUndoTestDisk(t)
		table := createUndoTestTable(t, bp)
		lockMgr := lock.NewManager(5000)

		err := table.Insert(bp, 1, lockMgr, [][]byte{[]byte("a"), []byte("Alice")})
		assert.NoError(t, err)

		// WHEN: purgeLimit=100 でパージ実行 (deleteMark=0 なので対象外)
		pt := newTestPurgeThread(bp, lockMgr, table)
		err = pt.purgeDeleteMarked(table, 100)
		assert.NoError(t, err)

		// THEN: レコードが残っている
		records := collectPurgeTestRecords(t, bp, table)
		assert.Equal(t, 1, len(records))
	})

	t.Run("空テーブルに対してパージしてもエラーにならない", func(t *testing.T) {
		// GIVEN
		bp := initUndoTestDisk(t)
		table := createUndoTestTable(t, bp)
		lockMgr := lock.NewManager(5000)

		// WHEN
		pt := newTestPurgeThread(bp, lockMgr, table)
		err := pt.purgeDeleteMarked(table, 100)

		// THEN
		assert.NoError(t, err)
	})

	t.Run("パージ後にクラスタ化インデックスからレコードが物理削除されている", func(t *testing.T) {
		// GIVEN
		bp := initUndoTestDisk(t)
		table := createUndoTestTable(t, bp)
		lockMgr := lock.NewManager(5000)

		err := table.Insert(bp, 1, lockMgr, [][]byte{[]byte("a"), []byte("Alice")})
		assert.NoError(t, err)
		err = table.Insert(bp, 1, lockMgr, [][]byte{[]byte("b"), []byte("Bob")})
		assert.NoError(t, err)
		lockMgr.ReleaseAll(1)

		err = table.SoftDelete(bp, 2, lockMgr, [][]byte{[]byte("a"), []byte("Alice")})
		assert.NoError(t, err)
		lockMgr.ReleaseAll(2)

		// WHEN
		pt := newTestPurgeThread(bp, lockMgr, table)
		err = pt.purgeDeleteMarked(table, 3)
		assert.NoError(t, err)

		// THEN: B+Tree から物理削除されている
		targets, err := pt.collectPurgeTargets(table, ^TrxId(0))
		assert.NoError(t, err)
		assert.Equal(t, 0, len(targets))

		// 同じプライマリキーで再挿入できる
		err = table.Insert(bp, 3, lockMgr, [][]byte{[]byte("a"), []byte("Carol")})
		assert.NoError(t, err)
	})
}

func TestRunPurge(t *testing.T) {
	t.Run("delete-marked レコードの物理削除と undo ログの破棄が行われる", func(t *testing.T) {
		// GIVEN
		bp := initUndoTestDisk(t)
		undoLog, err := NewUndoManager(bp, nil, undoTestFileId)
		assert.NoError(t, err)
		lockMgr := lock.NewManager(5000)

		table := createUndoTestTable(t, bp)
		table.undoLog = undoLog

		trxMgr := NewTrxManager(undoLog, lockMgr, nil)

		// T1: INSERT → COMMIT
		trx1 := trxMgr.Begin()
		err = table.Insert(bp, trx1, lockMgr, [][]byte{[]byte("a"), []byte("Alice")})
		assert.NoError(t, err)
		assert.NoError(t, trxMgr.Commit(trx1))

		// T2: DELETE → COMMIT
		trx2 := trxMgr.Begin()
		err = table.SoftDelete(bp, trx2, lockMgr, [][]byte{[]byte("a"), []byte("Alice")})
		assert.NoError(t, err)
		assert.NoError(t, trxMgr.Commit(trx2))

		assert.NotNil(t, undoLog.GetRecords(trx2))

		// WHEN
		pt := NewPurgeThread(bp, trxMgr, undoLog, lockMgr, func() []*Table {
			return []*Table{table}
		})
		purgeLimit := trxMgr.PurgeLimit()
		committedIds := trxMgr.CommittedTrxIds()
		err = pt.RunPurge(purgeLimit, committedIds)
		assert.NoError(t, err)

		// THEN: 物理削除されている
		targets, err := pt.collectPurgeTargets(table, ^TrxId(0))
		assert.NoError(t, err)
		assert.Equal(t, 0, len(targets))

		// undo ログも破棄されている
		assert.Nil(t, undoLog.GetRecords(trx2))
	})
}

func TestPurgeThread(t *testing.T) {
	t.Run("Start と Stop が正常に動作する", func(t *testing.T) {
		// GIVEN
		bp := initUndoTestDisk(t)
		undoLog, err := NewUndoManager(bp, nil, undoTestFileId)
		assert.NoError(t, err)
		lockMgr := lock.NewManager(5000)
		trxMgr := NewTrxManager(undoLog, lockMgr, nil)
		table := createUndoTestTable(t, bp)

		pt := NewPurgeThread(bp, trxMgr, undoLog, lockMgr, func() []*Table {
			return []*Table{table}
		})

		// WHEN
		pt.Start()
		time.Sleep(100 * time.Millisecond)
		pt.Stop()

		// THEN: パニックせずに停止
	})

	t.Run("Stop を 2 回呼んでもパニックしない", func(t *testing.T) {
		// GIVEN
		bp := initUndoTestDisk(t)
		undoLog, err := NewUndoManager(bp, nil, undoTestFileId)
		assert.NoError(t, err)
		lockMgr := lock.NewManager(5000)
		trxMgr := NewTrxManager(undoLog, lockMgr, nil)
		table := createUndoTestTable(t, bp)

		pt := NewPurgeThread(bp, trxMgr, undoLog, lockMgr, func() []*Table {
			return []*Table{table}
		})

		// WHEN
		pt.Start()
		pt.Stop()

		// THEN
		assert.NotPanics(t, func() { pt.Stop() })
	})

	t.Run("パージが実行されて delete-marked レコードが物理削除される", func(t *testing.T) {
		// GIVEN
		bp := initUndoTestDisk(t)
		undoLog, err := NewUndoManager(bp, nil, undoTestFileId)
		assert.NoError(t, err)
		lockMgr := lock.NewManager(5000)
		trxMgr := NewTrxManager(undoLog, lockMgr, nil)

		table := createUndoTestTable(t, bp)
		table.undoLog = undoLog

		trx1 := trxMgr.Begin()
		err = table.Insert(bp, trx1, lockMgr, [][]byte{[]byte("a"), []byte("Alice")})
		assert.NoError(t, err)
		assert.NoError(t, trxMgr.Commit(trx1))

		trx2 := trxMgr.Begin()
		err = table.SoftDelete(bp, trx2, lockMgr, [][]byte{[]byte("a"), []byte("Alice")})
		assert.NoError(t, err)
		assert.NoError(t, trxMgr.Commit(trx2))

		assert.NotNil(t, undoLog.GetRecords(trx2))

		// WHEN: パージスレッドを起動して少し待つ
		pt := NewPurgeThread(bp, trxMgr, undoLog, lockMgr, func() []*Table {
			return []*Table{table}
		})
		pt.Start()
		time.Sleep(1500 * time.Millisecond)
		pt.Stop()

		// THEN: delete-marked レコードが物理削除されている
		targets, err := pt.collectPurgeTargets(table, ^TrxId(0))
		assert.NoError(t, err)
		assert.Equal(t, 0, len(targets))

		// undo ログも破棄されている
		assert.Nil(t, undoLog.GetRecords(trx2))
	})
}

// newTestPurgeThread は purgeDeleteMarked テスト用の PurgeThread を作成するヘルパー
func newTestPurgeThread(bp *buffer.BufferPool, lockMgr *lock.Manager, table *Table) *PurgeThread {
	return &PurgeThread{bp: bp, lockMgr: lockMgr, tables: func() []*Table { return []*Table{table} }}
}

func collectPurgeTestRecords(t *testing.T, bp *buffer.BufferPool, table *Table) [][][]byte {
	t.Helper()
	rv := NewReadView(0, nil, ^uint64(0))
	vr := NewVersionReader(nil)
	iter, err := table.Search(bp, rv, vr, RecordSearchModeStart{})
	assert.NoError(t, err)
	var records [][][]byte
	for {
		record, ok, err := iter.Next()
		assert.NoError(t, err)
		if !ok {
			break
		}
		records = append(records, record)
	}
	return records
}
