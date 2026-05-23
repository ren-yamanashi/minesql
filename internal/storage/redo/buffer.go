package redo

import (
	"sync"

	"github.com/ren-yamanashi/minesql/internal/storage/lock"
	"github.com/ren-yamanashi/minesql/internal/storage/page"
)

// Buffer は Redo ログの記録・フラッシュ・読み取りを管理する
type Buffer struct {
	mutex   sync.Mutex
	records []Record
	logFile *file
	nextLsn Lsn // 次に割り当てる LSN
}

func NewBuffer() (*Buffer, error) {
	file, err := newFile()
	if err != nil {
		return nil, err
	}
	// クラッシュリカバリ時: フラッシュ済み LSN の次から採番を再開する
	nextLsn := file.flushedLsn + 1
	return &Buffer{
		logFile: file,
		nextLsn: nextLsn,
		records: []Record{},
	}, nil
}

// AppendPageCopy はページ変更レコードを Redo ログバッファに記録する
func (b *Buffer) AppendPageCopy(trxId lock.TrxId, pageId page.Id, pg page.Page) Lsn {
	b.mutex.Lock()
	defer b.mutex.Unlock()
	return b.append(trxId, RecordTypePageWrite, pageId, pg)
}

// AppendCommit は COMMIT レコードを Redo ログバッファに記録する
func (b *Buffer) AppendCommit(trxId lock.TrxId) Lsn {
	b.mutex.Lock()
	defer b.mutex.Unlock()
	return b.append(trxId, RecordTypeCommit, page.Id{}, page.Page{})
}

// AppendRollback は ROLLBACK レコードを Redo ログバッファに記録する
func (b *Buffer) AppendRollback(trxId lock.TrxId) Lsn {
	b.mutex.Lock()
	defer b.mutex.Unlock()
	return b.append(trxId, RecordTypeRollback, page.Id{}, page.Page{})
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

	if len(b.records) == 0 {
		return nil
	}

	err := b.logFile.flushRecords(b.records)
	if err != nil {
		return err
	}

	b.records = []Record{}
	return nil
}

// Clear は Redo ログをクリアする
func (b *Buffer) Clear() error {
	b.mutex.Lock()
	defer b.mutex.Unlock()
	return b.logFile.clear()
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

	var bufferSize int
	for _, record := range b.records {
		bufferSize += record.serializedSize()
	}

	return fileSize + int64(bufferSize), nil
}

// append は新しい Redo レコードをバッファに追加し、対応する LSN を返す
func (b *Buffer) append(trxId lock.TrxId, rt RecordType, pageId page.Id, pg page.Page) Lsn {
	lsn := b.allocateLsn()
	b.records = append(b.records, Record{
		lsn:        lsn,
		trxId:      trxId,
		recordType: rt,
		pageId:     pageId,
		data:       page.Copy(pg),
	})
	return lsn
}

// allocateLsn は LSN を採番して返す
func (b *Buffer) allocateLsn() Lsn {
	lsn := b.nextLsn
	b.nextLsn++
	return lsn
}
