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
	logFile *File
	nextLsn Lsn // 次に割り当てる LSN
}

func NewBuffer() (*Buffer, error) {
	file, err := newFile()
	if err != nil {
		return nil, err
	}
	return &Buffer{
		logFile: file,
		nextLsn: 1, // LSN=0 は無効値
	}, nil
}

// AppendPageCopy はページ変更レコードを Redo ログバッファに記録する
func (b *Buffer) AppendPageCopy(trxId lock.TrxId, pageId page.PageId, pg page.Page) Lsn {
	b.mutex.Lock()
	defer b.mutex.Unlock()
	return b.append(trxId, RecordTypePageWrite, pageId, pg)
}

// AppendCommit は COMMIT レコードを Redo ログバッファに記録する
func (b *Buffer) AppendCommit(trxId lock.TrxId) Lsn {
	b.mutex.Lock()
	defer b.mutex.Unlock()
	return b.append(trxId, RecordTypeCommit, page.PageId{}, page.Page{})
}

// AppendRollback は ROLLBACK レコードを Redo ログバッファに記録する
func (b *Buffer) AppendRollback(trxId lock.TrxId) Lsn {
	b.mutex.Lock()
	defer b.mutex.Unlock()
	return b.append(trxId, RecordTypeRollback, page.PageId{}, page.Page{})
}

// ReadAll は全レコードを読み込む
func (b *Buffer) ReadAll() ([]Record, error) {
	b.mutex.Lock()
	defer b.mutex.Unlock()
	return b.logFile.readRecords(Lsn(0))
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
	b.logFile.checkPointLsn = lsn
	return b.logFile.writeHeader()
}

// CheckpointLsn はチェックポイント LSN を返す
func (b *Buffer) CheckpointLsn() Lsn {
	b.mutex.Lock()
	defer b.mutex.Unlock()
	return b.logFile.checkPointLsn
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

	b.records = nil
	return nil
}

// Clear は Redo ログをクリアする
func (b *Buffer) Clear() error {
	b.mutex.Lock()
	defer b.mutex.Unlock()
	return b.logFile.clear()
}

// TruncateBefore は指定 LSN 以前のレコードをファイルから切り詰める
func (b *Buffer) TruncateBefore(lsn Lsn) error {
	b.mutex.Lock()
	defer b.mutex.Unlock()
	return b.logFile.truncateBefore(lsn)
}

// Size は Redo バッファの概算サイズ (バイト数) を返す
func (b *Buffer) Size() int {
	b.mutex.Lock()
	defer b.mutex.Unlock()

	size := 0
	for _, record := range b.records {
		dataSize := 0
		if record.Data.Header != nil {
			dataSize = len(record.Data.ToBytes())
		}
		size += recordHeaderSize + dataSize
	}
	return size
}

// append は新しい Redo レコードをバッファに追加し、対応する LSN を返す
func (b *Buffer) append(trxId lock.TrxId, recordType RecordType, pageId page.PageId, pg page.Page) Lsn {
	lsn := b.allocateLsn()
	b.records = append(b.records, Record{
		Lsn:    lsn,
		TrxId:  trxId,
		Type:   recordType,
		PageId: pageId,
		Data:   pg,
	})
	return lsn
}

// allocateLsn は LSN を採番して返す
func (b *Buffer) allocateLsn() Lsn {
	lsn := b.nextLsn
	b.nextLsn++
	return lsn
}
