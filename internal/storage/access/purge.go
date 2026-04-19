package access

import (
	stdlog "log"
	"minesql/internal/storage/btree"
	"minesql/internal/storage/buffer"
	"minesql/internal/storage/encode"
	"minesql/internal/storage/lock"
	"time"
)

// purgeTrxId はパージ操作で使用するトランザクション ID
//
// パージはどのユーザートランザクションにも属さないため、専用の ID を使い、操作完了後にロックを解放する
const purgeTrxId lock.TrxId = 0

// PurgeThread はバックグラウンドで不要な delete-marked レコードと undo ログを回収する
type PurgeThread struct {
	bp         *buffer.BufferPool // B+Tree の読み書きに使用
	trxManager *TrxManager        // パージ閾値の算出とコミット済みトランザクションの取得に使用
	undoLog    *UndoManager       // undo ログの破棄に使用
	lockMgr    *lock.Manager      // 物理削除時の排他ロック取得に使用
	tables     func() []*Table    // テーブル一覧を返すコールバック (access → dictionary の循環依存を避けるため)
	interval   time.Duration      // パージの実行間隔
	ticker     *time.Ticker       // 定期実行用タイマー
	done       chan struct{}      // 停止シグナル
	stopped    chan struct{}      // goroutine 終了通知用
}

// NewPurgeThread は PurgeThread を生成する
func NewPurgeThread(bp *buffer.BufferPool, trxManager *TrxManager, undoLog *UndoManager, lockMgr *lock.Manager, tables func() []*Table) *PurgeThread {
	return &PurgeThread{
		bp:         bp,
		trxManager: trxManager,
		undoLog:    undoLog,
		lockMgr:    lockMgr,
		tables:     tables,
		interval:   1 * time.Second,
	}
}

// Start はバックグラウンド goroutine を起動する
func (pt *PurgeThread) Start() {
	pt.ticker = time.NewTicker(pt.interval)
	pt.done = make(chan struct{})
	pt.stopped = make(chan struct{})
	go pt.loop()
}

// Stop はバックグラウンド goroutine を停止し、終了を待つ
func (pt *PurgeThread) Stop() {
	if pt.done == nil {
		return
	}
	close(pt.done)
	<-pt.stopped
	pt.ticker.Stop()
	pt.done = nil
}

// loop はバックグラウンドで定期的にパージを実行する
func (pt *PurgeThread) loop() {
	defer close(pt.stopped)
	for {
		select {
		case <-pt.done:
			return
		case <-pt.ticker.C:
			purgeLimit := pt.trxManager.PurgeLimit()
			committedIds := pt.trxManager.CommittedTrxIds()
			if err := pt.RunPurge(purgeLimit, committedIds); err != nil {
				stdlog.Printf("purge thread: %v", err)
			}
		}
	}
}

// RunPurge はパージ閾値に基づいて delete-marked レコードの物理削除と undo ログの破棄を行う
func (pt *PurgeThread) RunPurge(purgeLimit lock.TrxId, committedTrxIds []lock.TrxId) error {
	// delete-marked レコードの物理削除
	for _, table := range pt.tables() {
		if err := pt.purgeDeleteMarked(table, purgeLimit); err != nil {
			return err
		}
	}
	// undo ログの破棄
	pt.undoLog.Purge(purgeLimit, committedTrxIds)
	return nil
}

// purgeDeleteMarked はテーブルから delete-marked かつ lastModified < purgeLimit のレコードを物理削除する
//
// B+Tree の走査中に物理削除するとイテレータが壊れるため、削除対象のキーを先に収集し、走査完了後にまとめて削除する
func (pt *PurgeThread) purgeDeleteMarked(table *Table, purgeLimit lock.TrxId) error {
	targets, err := pt.collectPurgeTargets(table, purgeLimit)
	if err != nil {
		return err
	}

	defer pt.lockMgr.ReleaseAll(purgeTrxId)
	for _, columns := range targets {
		if err := table.delete(pt.bp, purgeTrxId, pt.lockMgr, columns); err != nil {
			return err
		}
	}
	return nil
}

// collectPurgeTargets は B+Tree を走査し、パージ対象のレコードのカラムデータを収集する
func (pt *PurgeThread) collectPurgeTargets(table *Table, purgeLimit lock.TrxId) ([][][]byte, error) {
	btr := btree.NewBTree(table.MetaPageId)
	iter, err := btr.Search(pt.bp, btree.SearchModeStart{})
	if err != nil {
		return nil, err
	}

	var targets [][][]byte
	for {
		record, ok, err := iter.Next(pt.bp)
		if err != nil {
			return nil, err
		}
		if !ok {
			break
		}

		if record.HeaderBytes()[0] != 1 {
			continue
		}
		lastModified, _, _ := decodeRecordNonKey(record.NonKeyBytes())
		if lastModified >= purgeLimit {
			continue
		}

		var columns [][]byte
		encode.Decode(record.KeyBytes(), &columns)
		_, _, nonKeyColumns := decodeRecordNonKey(record.NonKeyBytes())
		encode.Decode(nonKeyColumns, &columns)
		targets = append(targets, columns)
	}

	return targets, nil
}
