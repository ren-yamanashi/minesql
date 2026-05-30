package access

import (
	"errors"
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ren-yamanashi/minesql/internal/storage/btree"
	"github.com/ren-yamanashi/minesql/internal/storage/buffer"
	"github.com/ren-yamanashi/minesql/internal/storage/lock"
	"github.com/ren-yamanashi/minesql/internal/storage/page"
	"github.com/ren-yamanashi/minesql/internal/storage/undo"
)

type Purge struct {
	bufferPool  *buffer.Pool
	transaction *TrxManager
	undoLog     *undo.Manager
	interval    time.Duration
	ticker      *time.Ticker
	done        chan struct{}
	stopped     chan struct{}
	stopOnce    sync.Once
	isRunning   atomic.Bool
}

func NewPurge(bp *buffer.Pool, trx *TrxManager, undoLog *undo.Manager) *Purge {
	return &Purge{
		bufferPool:  bp,
		transaction: trx,
		undoLog:     undoLog,
		interval:    1 * time.Second,
	}
}

func (p *Purge) Start() {
	if !p.isRunning.CompareAndSwap(false, true) {
		return
	}
	p.ticker = time.NewTicker(p.interval)
	p.done = make(chan struct{})
	p.stopped = make(chan struct{})
	p.stopOnce = sync.Once{}
	go p.loop()
}

func (p *Purge) Stop() {
	p.stopOnce.Do(func() {
		if !p.isRunning.Load() {
			return
		}
		close(p.done)
		<-p.stopped
		p.ticker.Stop()
		p.isRunning.Store(false)
	})
}

// loop はバックグラウンドで定期的に purge を呼び出す
func (p *Purge) loop() {
	defer close(p.stopped)
	for {
		select {
		case <-p.done:
			return
		case <-p.ticker.C:
			if err := p.purge(); err != nil {
				log.Printf("purge: %v", err)
			}
		}
	}
}

// purge はパージ閾値を算出し、Undo ログを走査してパージを実行する
func (p *Purge) purge() error {
	purgeLimit := p.transaction.OldestVisibleTrxId()
	purgableTrxIds := p.purgableTrxIds(purgeLimit)
	if len(purgableTrxIds) == 0 {
		return nil
	}

	// 論理削除済みレコードの物理削除
	entries := p.undoLog.CommittedEntries(purgableTrxIds)
	for _, entry := range entries {
		if err := p.purgeEntry(entry); err != nil {
			return err
		}
	}

	// パージ済みの Undo ログを破棄
	for _, trxId := range purgableTrxIds {
		p.undoLog.Discard(trxId)
	}
	return nil
}

// purgeEntry は 1 つの Undo エントリに対応するパージ操作を実行する
func (p *Purge) purgeEntry(entry undo.Entry) error {
	switch entry.RecordType() {
	case undo.RecordTypeDelete:
		return p.purgeDelete(entry.Record())
	case undo.RecordTypeUpdate:
		return p.purgeUpdate(entry.Record())
	default:
		return nil
	}
}

// purgeDelete は DELETE の Undo レコードからプライマリ + セカンダリの論理削除済みレコードを物理削除する
func (p *Purge) purgeDelete(record undo.Record) error {
	deleteRecord, ok := record.(undo.DeleteRecord)
	if !ok {
		return fmt.Errorf("purge: expected DeleteRecord, got %T", record)
	}
	fileId := record.TableFileId()

	if err := p.deletePrimaryRecord(fileId, deleteRecord.Record()); err != nil {
		return err
	}
	return p.deleteSecondaryRecords(fileId, deleteRecord.Record())
}

// purgeUpdate は UPDATE の Undo レコードからセカンダリの論理削除済みレコードを物理削除する
func (p *Purge) purgeUpdate(record undo.Record) error {
	updateRecord, ok := record.(undo.UpdateRecord)
	if !ok {
		return fmt.Errorf("purge: expected UpdateRecord, got %T", record)
	}
	fileId := record.TableFileId()

	// 更新前のレコードからセカンダリキーを参照して物理削除
	return p.deleteSecondaryRecords(fileId, updateRecord.PrevRecord())
}

// deletePrimaryRecord はプライマリレコードを物理削除する
func (p *Purge) deletePrimaryRecord(fileId page.FileId, record btree.Record) error {
	mtr := buffer.NewMtr(p.bufferPool)
	defer mtr.UnpinAll()
	piRecord, err := fetchPrimaryIndexRecord(p.transaction.catalog, p.bufferPool, fileId)
	if err != nil {
		return err
	}
	primaryTree := btree.NewTree(p.bufferPool, piRecord.MetaPageId())
	return primaryTree.Delete(mtr, record.Key())
}

// deleteSecondaryRecords は指定されたプライマリインデックスのレコードに対応するセカンダリインデックスの論理削除済みレコードを物理削除する
func (p *Purge) deleteSecondaryRecords(fileId page.FileId, record btree.Record) error {
	mtr := buffer.NewMtr(p.bufferPool)
	defer mtr.UnpinAll()
	prevRec, err := DecodePrimaryRecord(record, p.transaction.catalog, p.bufferPool, fileId)
	if err != nil {
		return err
	}

	siRecords, err := fetchSecondaryIndexRecords(p.transaction.catalog, p.bufferPool, fileId)
	if err != nil {
		return err
	}

	for _, siRecord := range siRecords {
		keyCols, err := fetchIndexKeyColumn(p.transaction.catalog, p.bufferPool, siRecord.IndexId())
		if err != nil {
			return err
		}
		sk := prevRec.SecondaryKey(keyCols)
		tree := btree.NewTree(p.bufferPool, siRecord.MetaPageId())

		// キーが存在し、deleteMark=1 の場合のみ物理削除
		existing, _, err := tree.FindByKey(mtr, sk)
		if errors.Is(err, btree.ErrKeyNotFound) {
			continue
		}
		if err != nil {
			return err
		}
		if existing.Header()[0] == 0 {
			continue
		}
		if err := tree.Delete(mtr, sk); err != nil {
			return err
		}
	}
	return nil
}

// purgableTrxIds は完了済みトランザクションのうち purgeLimit 未満の ID を返す
func (p *Purge) purgableTrxIds(purgeLimit lock.TrxId) []lock.TrxId {
	var ids []lock.TrxId
	for _, trxId := range p.transaction.InactiveTrxIds() {
		if trxId < purgeLimit {
			ids = append(ids, trxId)
		}
	}
	return ids
}
