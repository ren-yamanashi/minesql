package redo

import (
	"errors"
	"math"
	"sync"

	"github.com/ren-yamanashi/minesql/internal/storage/lock"
	"github.com/ren-yamanashi/minesql/internal/storage/page"
)

const maxBufferSize = 4 * 1024 * 1024 // 4MB

var ErrLsnOverflow = errors.New("redo: lsn overflow")

type Buffer struct {
	mutex       sync.Mutex
	records     []Record
	logFile     *file
	nextLsn     Lsn // 次に割り当てる LSN
	pendingSize int // バッファ内の未フラッシュレコードの合計バイト数
}

func NewBuffer(baseDir string) (*Buffer, error) {
	file, err := newFile(baseDir)
	if err != nil {
		return nil, err
	}
	// クラッシュリカバリ時: フラッシュ済み LSN の次から採番を再開する
	nextLsn := file.flushedLsn + 1
	return &Buffer{
		logFile: file,
		nextLsn: nextLsn,
	}, nil
}

// AppendPageCopy はページ変更レコードを Redo ログバッファに記録する
func (b *Buffer) AppendPageCopy(trxId lock.TrxId, pageId page.Id, pg *page.Page) (Lsn, error) {
	b.mutex.Lock()
	defer b.mutex.Unlock()
	return b.appendRecord(trxId, RecordTypePageWrite, pageId, pg)
}

// AppendCommit は COMMIT レコードを Redo ログバッファに記録する
func (b *Buffer) AppendCommit(trxId lock.TrxId) (Lsn, error) {
	b.mutex.Lock()
	defer b.mutex.Unlock()
	return b.appendRecord(trxId, RecordTypeCommit, page.Id{}, nil)
}

// AppendRollback は ROLLBACK レコードを Redo ログバッファに記録する
func (b *Buffer) AppendRollback(trxId lock.TrxId) (Lsn, error) {
	b.mutex.Lock()
	defer b.mutex.Unlock()
	return b.appendRecord(trxId, RecordTypeRollback, page.Id{}, nil)
}

// ReadFrom は指定 LSN より大きい LSN を持つレコードを読み込む
func (b *Buffer) ReadFrom(lsn Lsn) ([]Record, error) {
	b.mutex.Lock()
	defer b.mutex.Unlock()
	return b.logFile.readRecords(lsn)
}

// SetCheckpointLsn はチェックポイント LSN を更新し、ヘッダーに書き込む
func (b *Buffer) SetCheckpointLsn(lsn Lsn) error {
	b.mutex.Lock()
	defer b.mutex.Unlock()
	return b.logFile.setCheckpointLsn(lsn)
}

// CheckpointLsn はチェックポイント LSN を返す
func (b *Buffer) CheckpointLsn() Lsn {
	b.mutex.Lock()
	defer b.mutex.Unlock()
	return b.logFile.checkpointLsn
}

// FlushedLsn はディスクにフラッシュ済みの最大 LSN を返す
func (b *Buffer) FlushedLsn() Lsn {
	b.mutex.Lock()
	defer b.mutex.Unlock()
	return b.logFile.flushedLsn
}

// Flush はバッファの全レコードをディスクに書き込む
func (b *Buffer) Flush() error {
	b.mutex.Lock()
	defer b.mutex.Unlock()
	return b.flush()
}

// Clear は Redo ログをクリアする
func (b *Buffer) Clear() error {
	b.mutex.Lock()
	defer b.mutex.Unlock()
	if err := b.logFile.clear(); err != nil {
		return err
	}
	b.records = nil
	b.pendingSize = 0
	b.nextLsn = 1
	return nil
}

// Close はバッファが保持するファイルリソースを解放する
func (b *Buffer) Close() error {
	b.mutex.Lock()
	defer b.mutex.Unlock()
	return b.logFile.close()
}

// TruncateBefore は指定 LSN 以前のレコードをファイルから切り詰める
func (b *Buffer) TruncateBefore(lsn Lsn) error {
	b.mutex.Lock()
	defer b.mutex.Unlock()
	return b.logFile.truncateBefore(lsn)
}

// Size は Redo ログのサイズ (ファイルサイズ + バッファサイズ) を返す
func (b *Buffer) Size() (int64, error) {
	b.mutex.Lock()
	defer b.mutex.Unlock()

	fileSize, err := b.logFile.size()
	if err != nil {
		return 0, err
	}

	return fileSize + int64(b.pendingSize), nil
}

// appendRecord は新しい Redo レコードをバッファに追加し、対応する LSN を返す
// バッファサイズが上限を超えた場合は自動的にフラッシュする
func (b *Buffer) appendRecord(trxId lock.TrxId, rt RecordType, pageId page.Id, pg *page.Page) (Lsn, error) {
	prevNextLsn := b.nextLsn
	prevPendingSize := b.pendingSize
	prevRecordsLen := len(b.records)

	lsn, err := b.allocateLsn()
	if err != nil {
		return 0, err
	}

	var data *page.Page
	if !pg.IsZero() {
		data = pg.Copy()
	}
	rec := Record{
		lsn:        lsn,
		trxId:      trxId,
		recordType: rt,
		pageId:     pageId,
		data:       data,
	}
	b.records = append(b.records, rec)
	b.pendingSize += rec.Size()

	// バッファサイズが上限を超えた場合は自動フラッシュ
	if b.pendingSize >= maxBufferSize {
		if err := b.flush(); err != nil {
			b.records = b.records[:prevRecordsLen]
			b.pendingSize = prevPendingSize
			b.nextLsn = prevNextLsn
			return 0, err
		}
	}

	return lsn, nil
}

// flush はバッファの全レコードをディスクに書き込む (ロックなし、内部用)
func (b *Buffer) flush() error {
	if len(b.records) == 0 {
		return nil
	}

	if err := b.logFile.flushRecords(b.records); err != nil {
		return err
	}

	b.records = nil
	b.pendingSize = 0
	return nil
}

// allocateLsn は LSN を採番して返す
func (b *Buffer) allocateLsn() (Lsn, error) {
	if b.nextLsn == math.MaxUint32 {
		return 0, ErrLsnOverflow
	}
	lsn := b.nextLsn
	b.nextLsn++
	return lsn, nil
}
