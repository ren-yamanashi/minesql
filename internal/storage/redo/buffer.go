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
	if file.flushedLsn == math.MaxUint32 {
		return nil, errors.Join(ErrLsnOverflow, file.close())
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
	return b.appendRecord(trxId, RecordTypePageWrite, pageId, pg, nil)
}

// AppendPageWrite はページ変更レコードを Redo ログバッファに記録する
//   - stamp は採番した LSN を受け取りページヘッダーへ書き込むコールバックで、ページ全体コピーの前に呼ばれる
//   - 呼び出し側は pageId に対して X latch を保持していること
func (b *Buffer) AppendPageWrite(trxId lock.TrxId, pageId page.Id, pg *page.Page, stamp func(Lsn)) (Lsn, error) {
	b.mutex.Lock()
	defer b.mutex.Unlock()
	return b.appendRecord(trxId, RecordTypePageWrite, pageId, pg, stamp)
}

// AppendCommit は COMMIT レコードを Redo ログバッファに記録する
func (b *Buffer) AppendCommit(trxId lock.TrxId) (Lsn, error) {
	b.mutex.Lock()
	defer b.mutex.Unlock()
	return b.appendRecord(trxId, RecordTypeCommit, page.Id{}, nil, nil)
}

// AppendRollback は ROLLBACK レコードを Redo ログバッファに記録する
func (b *Buffer) AppendRollback(trxId lock.TrxId) (Lsn, error) {
	b.mutex.Lock()
	defer b.mutex.Unlock()
	return b.appendRecord(trxId, RecordTypeRollback, page.Id{}, nil, nil)
}

// AppendMtrStart は mini-transaction の開始マーカーを Redo ログバッファに記録する
func (b *Buffer) AppendMtrStart(trxId lock.TrxId) (Lsn, error) {
	b.mutex.Lock()
	defer b.mutex.Unlock()
	return b.appendRecord(trxId, RecordTypeMtrStart, page.Id{}, nil, nil)
}

// AppendMtrEnd は mini-transaction の終了マーカーを Redo ログバッファに記録する
func (b *Buffer) AppendMtrEnd(trxId lock.TrxId) (Lsn, error) {
	b.mutex.Lock()
	defer b.mutex.Unlock()
	return b.appendRecord(trxId, RecordTypeMtrEnd, page.Id{}, nil, nil)
}

// ReadFrom は指定 LSN より大きい LSN を持つレコードを読み込む
func (b *Buffer) ReadFrom(lsn Lsn) ([]Record, error) {
	b.mutex.Lock()
	defer b.mutex.Unlock()
	return b.logFile.readRecords(lsn)
}

// MaxUserTrxId は Redo ログ全体を走査し、出現するユーザートランザクション ID の最大値を返す
//   - システム予約トランザクション ID と Purge 予約トランザクション ID は除外する
//   - ユーザートランザクションが 1 件も見つからない場合は 0 を返す
func (b *Buffer) MaxUserTrxId() (lock.TrxId, error) {
	b.mutex.Lock()
	defer b.mutex.Unlock()
	records, err := b.logFile.readRecords(0)
	if err != nil {
		return 0, err
	}
	var maxId lock.TrxId
	for _, r := range records {
		id := r.trxId
		if id == lock.SystemReservedTrxId || id == lock.PurgeReservedTrxId {
			continue
		}
		if id > maxId {
			maxId = id
		}
	}
	return maxId, nil
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
//   - stamp が非 nil の場合、採番した LSN を渡して呼び出してから pg をコピーする (ページヘッダーへの Page LSN スタンプ用)
func (b *Buffer) appendRecord(trxId lock.TrxId, rt RecordType, pageId page.Id, pg *page.Page, stamp func(Lsn)) (Lsn, error) {
	prevNextLsn := b.nextLsn
	prevPendingSize := b.pendingSize
	prevRecordsLen := len(b.records)

	lsn, err := b.allocateLsn()
	if err != nil {
		return 0, err
	}

	if stamp != nil {
		stamp(lsn)
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
