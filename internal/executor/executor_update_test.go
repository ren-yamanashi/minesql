package executor

import (
	"minesql/internal/storage/access"
	"minesql/internal/storage/handler"
	"minesql/internal/storage/lock"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestNewUpdate(t *testing.T) {
	t.Run("正常に Update Executor を生成できる", func(t *testing.T) {
		// GIVEN
		var trxId handler.TrxId = 1
		setColumns := []SetColumn{
			{Pos: 1, Value: []byte("Jane")},
		}

		iterator := NewTableScan(
			0, lock.NewManager(5000), nil,
			access.RecordSearchModeStart{},
			func(record Record) bool { return true },
		)

		// WHEN
		upd := NewUpdate(trxId, nil, setColumns, iterator)

		// THEN
		assert.NotNil(t, upd)
	})
}

func TestUpdate_Next(t *testing.T) {
	t.Run("全レコードの value を更新できる", func(t *testing.T) {
		// GIVEN
		tmpdir := t.TempDir()
		InitStorageEngineForTest(t, tmpdir)
		defer handler.Reset()

		var trxId handler.TrxId = 1

		// テーブルアクセスメソッドを取得
		tbl, err := getTable("users")
		assert.NoError(t, err)

		upd := NewUpdate(trxId, tbl, []SetColumn{
			{Pos: 1, Value: []byte("Updated")},
		}, NewTableScan(
			0, lock.NewManager(5000), tbl,
			access.RecordSearchModeStart{},
			func(record Record) bool { return true },
		))

		// WHEN
		_, err = upd.Next()

		// THEN: 更新が成功する
		assert.NoError(t, err)

		// THEN: 全レコードの first_name が "Updated" になっている
		scan := NewTableScan(
			0, lock.NewManager(5000), tbl,
			access.RecordSearchModeStart{},
			func(record Record) bool { return true },
		)
		results, err := fetchAll(scan)
		assert.NoError(t, err)
		assert.Equal(t, 5, len(results))
		for _, record := range results {
			assert.Equal(t, "Updated", string(record[1]))
		}
	})

	t.Run("条件付きで一部のレコードを更新できる", func(t *testing.T) {
		// GIVEN
		tmpdir := t.TempDir()
		InitStorageEngineForTest(t, tmpdir)
		defer handler.Reset()

		var trxId handler.TrxId = 1

		// テーブルアクセスメソッドを取得
		tbl, err := getTable("users")
		assert.NoError(t, err)

		// プライマリキーが "a" のレコードのみ更新
		upd := NewUpdate(trxId, tbl, []SetColumn{
			{Pos: 1, Value: []byte("Jane")},
			{Pos: 2, Value: []byte("Updated")},
		}, NewTableScan(
			0, lock.NewManager(5000), tbl,
			access.RecordSearchModeKey{Key: [][]byte{[]byte("a")}},
			func(record Record) bool {
				return string(record[0]) == "a"
			},
		))

		// WHEN
		_, err = upd.Next()

		// THEN: 更新が成功する
		assert.NoError(t, err)

		// THEN: "a" のレコードが更新され、他は変わらない
		scan := NewTableScan(
			0, lock.NewManager(5000), tbl,
			access.RecordSearchModeStart{},
			func(record Record) bool { return true },
		)
		results, err := fetchAll(scan)
		assert.NoError(t, err)
		assert.Equal(t, 5, len(results))
		assert.Equal(t, Record{[]byte("a"), []byte("Jane"), []byte("Updated")}, results[0])
		assert.Equal(t, Record{[]byte("b"), []byte("Alice"), []byte("Smith")}, results[1])
	})

	t.Run("Filter を組み合わせて特定の条件のレコードを更新できる", func(t *testing.T) {
		// GIVEN
		tmpdir := t.TempDir()
		InitStorageEngineForTest(t, tmpdir)
		defer handler.Reset()

		var trxId handler.TrxId = 1

		// テーブルアクセスメソッドを取得
		tbl, err := getTable("users")
		assert.NoError(t, err)

		// first_name が "Bob" のレコードの last_name を更新
		upd := NewUpdate(trxId, tbl, []SetColumn{
			{Pos: 2, Value: []byte("Williams")},
		}, NewFilter(
			NewTableScan(
				0, lock.NewManager(5000), tbl,
				access.RecordSearchModeStart{},
				func(record Record) bool { return true },
			),
			func(record Record) bool {
				return string(record[1]) == "Bob"
			},
		))

		// WHEN
		_, err = upd.Next()

		// THEN: 更新が成功する
		assert.NoError(t, err)

		// THEN: "Bob" の last_name が "Williams" に更新され、他は変わらない
		scan := NewTableScan(
			0, lock.NewManager(5000), tbl,
			access.RecordSearchModeStart{},
			func(record Record) bool { return true },
		)
		results, err := fetchAll(scan)
		assert.NoError(t, err)
		assert.Equal(t, 5, len(results))
		// "c" = Bob のレコード
		assert.Equal(t, Record{[]byte("c"), []byte("Bob"), []byte("Williams")}, results[2])
		// 他のレコードは変わらない
		assert.Equal(t, []byte("Doe"), results[0][2])
		assert.Equal(t, []byte("Smith"), results[1][2])
		assert.Equal(t, []byte("Davis"), results[3][2])
		assert.Equal(t, []byte("Brown"), results[4][2])
	})

	t.Run("更新後にユニークインデックスも更新されている", func(t *testing.T) {
		// GIVEN
		tmpdir := t.TempDir()
		InitStorageEngineForTest(t, tmpdir)
		defer handler.Reset()

		var trxId handler.TrxId = 1

		// テーブルアクセスメソッドを取得
		tbl, err := getTable("users")
		assert.NoError(t, err)

		// インデックスアクセスメソッドを取得
		idx, err := tbl.GetUniqueIndexByName("last_name")
		assert.NoError(t, err)

		// "a" (last_name = "Doe") の last_name を "Zebra" に更新
		upd := NewUpdate(trxId, tbl, []SetColumn{
			{Pos: 2, Value: []byte("Zebra")},
		}, NewTableScan(
			0, lock.NewManager(5000), tbl,
			access.RecordSearchModeKey{Key: [][]byte{[]byte("a")}},
			func(record Record) bool {
				return string(record[0]) == "a"
			},
		))

		// WHEN
		_, err = upd.Next()

		// THEN: 更新が成功する
		assert.NoError(t, err)

		// THEN: ユニークインデックスで "Zebra" が検索できる
		// SearchIndex の whileCondition にはデコードされたセカンダリキーのみ渡される
		indexScan := NewIndexScan(
			tbl,
			idx,
			access.RecordSearchModeKey{Key: [][]byte{[]byte("Zebra")}},
			func(record Record) bool {
				return string(record[0]) == "Zebra"
			},
		)
		results, err := fetchAll(indexScan)
		assert.NoError(t, err)
		assert.Equal(t, 1, len(results))
		assert.Equal(t, Record{[]byte("a"), []byte("John"), []byte("Zebra")}, results[0])

		// THEN: ユニークインデックスで旧値 "Doe" が検索できない
		indexScanOld := NewIndexScan(
			tbl,
			idx,
			access.RecordSearchModeKey{Key: [][]byte{[]byte("Doe")}},
			func(record Record) bool {
				return string(record[0]) == "Doe"
			},
		)
		resultsOld, err := fetchAll(indexScanOld)
		assert.NoError(t, err)
		assert.Equal(t, 0, len(resultsOld))
	})

	t.Run("プライマリキーカラムを更新できる", func(t *testing.T) {
		// GIVEN
		tmpdir := t.TempDir()
		InitStorageEngineForTest(t, tmpdir)
		defer handler.Reset()

		var trxId handler.TrxId = 1

		// テーブルアクセスメソッドを取得
		tbl, err := getTable("users")
		assert.NoError(t, err)

		// プライマリキーを "a" → "z" に変更
		upd := NewUpdate(trxId, tbl, []SetColumn{
			{Pos: 0, Value: []byte("z")},
		}, NewTableScan(
			0, lock.NewManager(5000), tbl,
			access.RecordSearchModeKey{Key: [][]byte{[]byte("a")}},
			func(record Record) bool {
				return string(record[0]) == "a"
			},
		))

		// WHEN
		_, err = upd.Next()

		// THEN: 更新が成功する
		assert.NoError(t, err)

		// THEN: "a" が消え "z" が追加されている
		scan := NewTableScan(
			0, lock.NewManager(5000), tbl,
			access.RecordSearchModeStart{},
			func(record Record) bool { return true },
		)
		results, err := fetchAll(scan)
		assert.NoError(t, err)
		assert.Equal(t, 5, len(results))
		// "a" は存在しない
		assert.Equal(t, []byte("b"), results[0][0])
		// "z" が末尾にある
		assert.Equal(t, []byte("z"), results[4][0])
		assert.Equal(t, []byte("John"), results[4][1])
		assert.Equal(t, []byte("Doe"), results[4][2])
	})

	t.Run("条件に一致するレコードがない場合、何も更新されない", func(t *testing.T) {
		// GIVEN
		tmpdir := t.TempDir()
		InitStorageEngineForTest(t, tmpdir)
		defer handler.Reset()

		var trxId handler.TrxId = 1

		// テーブルアクセスメソッドを取得
		tbl, err := getTable("users")
		assert.NoError(t, err)

		// 存在しない first_name でフィルタ
		upd := NewUpdate(trxId, tbl, []SetColumn{
			{Pos: 2, Value: []byte("Changed")},
		}, NewFilter(
			NewTableScan(
				0, lock.NewManager(5000), tbl,
				access.RecordSearchModeStart{},
				func(record Record) bool { return true },
			),
			func(record Record) bool {
				return string(record[1]) == "NonExistent"
			},
		))

		// WHEN
		_, err = upd.Next()

		// THEN: エラーなしで正常終了
		assert.NoError(t, err)

		// THEN: 全レコードが変更されていない
		scan := NewTableScan(
			0, lock.NewManager(5000), tbl,
			access.RecordSearchModeStart{},
			func(record Record) bool { return true },
		)
		results, err := fetchAll(scan)
		assert.NoError(t, err)
		assert.Equal(t, 5, len(results))
		assert.Equal(t, []byte("Doe"), results[0][2])
		assert.Equal(t, []byte("Smith"), results[1][2])
		assert.Equal(t, []byte("Johnson"), results[2][2])
		assert.Equal(t, []byte("Davis"), results[3][2])
		assert.Equal(t, []byte("Brown"), results[4][2])
	})

	t.Run("空のテーブルに対して更新しても正常に動作する", func(t *testing.T) {
		// GIVEN
		tmpdir := t.TempDir()
		initStorageManagerForTest(t)
		defer handler.Reset()
		_ = tmpdir

		var trxId handler.TrxId = 1
		createTableForTest(t, "empty_table", nil, []handler.CreateColumnParam{
			{Name: "id", Type: handler.ColumnTypeString},
			{Name: "value", Type: handler.ColumnTypeString},
		})

		// テーブルアクセスメソッドを取得
		tbl, err := getTable("empty_table")
		assert.NoError(t, err)

		upd := NewUpdate(trxId, tbl, []SetColumn{
			{Pos: 1, Value: []byte("new_value")},
		}, NewTableScan(
			0, lock.NewManager(5000), tbl,
			access.RecordSearchModeStart{},
			func(record Record) bool { return true },
		))

		// WHEN
		_, err = upd.Next()

		// THEN
		assert.NoError(t, err)
	})

	t.Run("UPDATE 中に同じ行を UPDATE しようとするとブロックされる", func(t *testing.T) {
		// GIVEN
		initLockTestHandler(t)
		defer handler.Reset()

		hdl := handler.Get()
		tbl := createLockTestTable(t)
		insertLockTestData(t, tbl)

		trx1 := hdl.BeginTrx()

		// trx1 が UPDATE を実行 (排他ロック取得)
		upd1 := NewUpdate(trx1, tbl, []SetColumn{
			{Pos: 1, Value: []byte("Updated1")},
		}, NewTableScan(
			trx1, hdl.LockMgr, tbl,
			access.RecordSearchModeStart{},
			func(record Record) bool { return string(record[0]) == "a" },
		))
		_, err := upd1.Next()
		assert.NoError(t, err)

		// WHEN: trx2 が同じ行を UPDATE しようとする (排他ロック待ち → タイムアウト)
		var wg sync.WaitGroup
		var updateErr error
		wg.Add(1)
		go func() {
			defer wg.Done()
			trx2 := hdl.BeginTrx()
			upd2 := NewUpdate(trx2, tbl, []SetColumn{
				{Pos: 1, Value: []byte("Updated2")},
			}, NewTableScan(
				trx2, hdl.LockMgr, tbl,
				access.RecordSearchModeStart{},
				func(record Record) bool { return string(record[0]) == "a" },
			))
			_, updateErr = upd2.Next()
		}()

		wg.Wait()

		// THEN
		assert.ErrorIs(t, updateErr, lock.ErrTimeout)

		hdl.CommitTrx(trx1)
	})

	t.Run("COMMIT 後は他のトランザクションが排他ロックを取得できる", func(t *testing.T) {
		// GIVEN
		initLockTestHandler(t)
		defer handler.Reset()

		hdl := handler.Get()
		tbl := createLockTestTable(t)
		insertLockTestData(t, tbl)

		trx1 := hdl.BeginTrx()

		// trx1 が UPDATE (排他ロック取得)
		upd := NewUpdate(trx1, tbl, []SetColumn{
			{Pos: 1, Value: []byte("Updated")},
		}, NewTableScan(
			trx1, hdl.LockMgr, tbl,
			access.RecordSearchModeStart{},
			func(record Record) bool { return string(record[0]) == "a" },
		))
		_, err := upd.Next()
		assert.NoError(t, err)

		// WHEN: trx1 を COMMIT (ロック解放)
		hdl.CommitTrx(trx1)

		// THEN: trx2 が同じ行を UPDATE できる
		trx2 := hdl.BeginTrx()
		upd2 := NewUpdate(trx2, tbl, []SetColumn{
			{Pos: 1, Value: []byte("Updated2")},
		}, NewTableScan(
			trx2, hdl.LockMgr, tbl,
			access.RecordSearchModeStart{},
			func(record Record) bool { return string(record[0]) == "a" },
		))
		_, err = upd2.Next()
		assert.NoError(t, err)

		hdl.CommitTrx(trx2)
	})

	t.Run("排他ロック解放後に待機中のトランザクションがロックを取得できる", func(t *testing.T) {
		// GIVEN
		initLockTestHandler(t)
		defer handler.Reset()

		hdl := handler.Get()
		tbl := createLockTestTable(t)
		insertLockTestData(t, tbl)

		trx1 := hdl.BeginTrx()

		// trx1 が UPDATE (排他ロック取得)
		upd := NewUpdate(trx1, tbl, []SetColumn{
			{Pos: 1, Value: []byte("Updated1")},
		}, NewTableScan(
			trx1, hdl.LockMgr, tbl,
			access.RecordSearchModeStart{},
			func(record Record) bool { return string(record[0]) == "a" },
		))
		_, err := upd.Next()
		assert.NoError(t, err)

		// WHEN: trx2 が同じ行を UPDATE しようとする (別 goroutine で待機)
		var wg sync.WaitGroup
		var updateErr error
		wg.Add(1)
		go func() {
			defer wg.Done()
			trx2 := hdl.BeginTrx()
			upd2 := NewUpdate(trx2, tbl, []SetColumn{
				{Pos: 1, Value: []byte("Updated2")},
			}, NewTableScan(
				trx2, hdl.LockMgr, tbl,
				access.RecordSearchModeStart{},
				func(record Record) bool { return string(record[0]) == "a" },
			))
			_, updateErr = upd2.Next()
			hdl.CommitTrx(trx2)
		}()

		// trx1 を COMMIT して排他ロックを解放
		time.Sleep(50 * time.Millisecond)
		hdl.CommitTrx(trx1)

		wg.Wait()

		// THEN: trx2 がロックを取得でき、UPDATE が成功する
		assert.NoError(t, updateErr)

		// 更新後のデータを確認
		records := collectLockTestRecords(t, tbl)
		assert.Equal(t, 2, len(records))
		assert.Equal(t, "Updated2", string(records[0][1]))
	})

	t.Run("ROLLBACK 後はデータが元に戻り、他のトランザクションが更新できる", func(t *testing.T) {
		// GIVEN
		initLockTestHandler(t)
		defer handler.Reset()

		hdl := handler.Get()
		tbl := createLockTestTable(t)
		insertLockTestData(t, tbl)

		trx1 := hdl.BeginTrx()

		// trx1 が UPDATE (排他ロック取得)
		upd := NewUpdate(trx1, tbl, []SetColumn{
			{Pos: 1, Value: []byte("RolledBack")},
		}, NewTableScan(
			trx1, hdl.LockMgr, tbl,
			access.RecordSearchModeStart{},
			func(record Record) bool { return string(record[0]) == "a" },
		))
		_, err := upd.Next()
		assert.NoError(t, err)

		// WHEN: trx1 を ROLLBACK (ロック解放 + データ巻き戻し)
		err = hdl.RollbackTrx(trx1)
		assert.NoError(t, err)

		// THEN: データが元に戻っている
		records := collectLockTestRecords(t, tbl)
		assert.Equal(t, 2, len(records))
		assert.Equal(t, "Alice", string(records[0][1]))

		// THEN: trx2 が同じ行を UPDATE できる (ロックが解放されている)
		trx2 := hdl.BeginTrx()
		upd2 := NewUpdate(trx2, tbl, []SetColumn{
			{Pos: 1, Value: []byte("Updated")},
		}, NewTableScan(
			trx2, hdl.LockMgr, tbl,
			access.RecordSearchModeStart{},
			func(record Record) bool { return string(record[0]) == "a" },
		))
		_, err = upd2.Next()
		assert.NoError(t, err)
		hdl.CommitTrx(trx2)
	})
}

// initLockTestHandler はロックテスト用に handler を初期化する (タイムアウト短め)
func initLockTestHandler(t *testing.T) {
	t.Helper()
	tmpdir := t.TempDir()
	t.Setenv("MINESQL_DATA_DIR", tmpdir)
	t.Setenv("MINESQL_BUFFER_SIZE", "10")
	t.Setenv("MINESQL_LOCK_WAIT_TIMEOUT", "200")
	handler.Reset()
	handler.Init()
}

func createLockTestTable(t *testing.T) *access.Table {
	t.Helper()
	createTable := NewCreateTable("lock_test", 1, nil, []handler.CreateColumnParam{
		{Name: "id", Type: handler.ColumnTypeString},
		{Name: "name", Type: handler.ColumnTypeString},
	})
	_, err := createTable.Next()
	assert.NoError(t, err)

	hdl := handler.Get()
	tblMeta, ok := hdl.Catalog.GetTableMetaByName("lock_test")
	assert.True(t, ok)
	tbl, err := tblMeta.GetTable()
	assert.NoError(t, err)
	return tbl
}

func insertLockTestData(t *testing.T, tbl *access.Table) {
	t.Helper()
	hdl := handler.Get()
	trxId := hdl.BeginTrx()
	ins := NewInsert(trxId, tbl, []Record{
		{[]byte("a"), []byte("Alice")},
		{[]byte("b"), []byte("Bob")},
	})
	_, err := ins.Next()
	assert.NoError(t, err)
	hdl.CommitTrx(trxId)
}

func collectLockTestRecords(t *testing.T, tbl *access.Table) []Record {
	t.Helper()
	hdl := handler.Get()
	trx := hdl.BeginTrx()
	scan := NewTableScan(
		trx, hdl.LockMgr, tbl,
		access.RecordSearchModeStart{},
		func(record Record) bool { return true },
	)
	var records []Record
	for {
		record, err := scan.Next()
		assert.NoError(t, err)
		if record == nil {
			break
		}
		records = append(records, record)
	}
	hdl.CommitTrx(trx)
	return records
}
