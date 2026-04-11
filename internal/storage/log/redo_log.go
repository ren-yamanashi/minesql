package log

import (
	"encoding/binary"
	"fmt"
	"io"
	"minesql/internal/storage/page"
	"os"
	"path/filepath"
)

const (
	redoLogFileName   = "redo.log"
	redoLogHeaderSize = 16 // Flushed LSN (4B) + 予約 (12B)
)

// RedoLog は REDO ログの記録・フラッシュ・読み取りを管理する
type RedoLog struct {
	lsnGen     *LSNGenerator
	buffer     []RedoRecord // メモリ上の REDO ログバッファ
	file       *os.File     // redo.log ファイル
	FlushedLSN LSN          // ディスクにフラッシュ済みの最大 LSN
}

// NewRedoLog は redo.log を開く (存在しない場合は新規作成する)
//
// ファイルヘッダーから FlushedLSN を復元し、LSNGenerator の初期値にする
func NewRedoLog(dataDir string) (*RedoLog, error) {
	filePath := filepath.Join(dataDir, redoLogFileName)
	file, err := os.OpenFile(filepath.Clean(filePath), os.O_RDWR|os.O_CREATE, 0600)
	if err != nil {
		return nil, fmt.Errorf("failed to open redo log: %w", err)
	}

	rl := &RedoLog{file: file}

	// ファイルヘッダーから FlushedLSN を読み取る
	stat, err := file.Stat()
	if err != nil {
		return nil, err
	}

	// ファイルサイズがヘッダーサイズ以上ならヘッダーを読み取る。そうでなければ新規ファイルとしてヘッダーを書き込む
	if stat.Size() >= redoLogHeaderSize {
		header := make([]byte, redoLogHeaderSize)
		if _, err := file.ReadAt(header, 0); err != nil {
			return nil, err
		}
		rl.FlushedLSN = LSN(binary.BigEndian.Uint32(header[0:4]))
	} else {
		// 新規ファイルなのでヘッダーを書き込む
		if err := rl.writeHeader(); err != nil {
			return nil, err
		}
	}

	rl.lsnGen = NewLSNGenerator(rl.FlushedLSN)

	return rl, nil
}

// AppendPageCopy はページ全体のコピーを REDO ログバッファに記録する
func (rl *RedoLog) AppendPageCopy(trxId uint64, pageId page.PageId, data []byte) LSN {
	return rl.append(trxId, RedoPageWrite, pageId, data)
}

// AppendCommit は COMMIT レコードを REDO ログバッファに記録する
func (rl *RedoLog) AppendCommit(trxId uint64) LSN {
	return rl.append(trxId, RedoCommit, page.PageId{}, nil)
}

// AppendRollback は ROLLBACK レコードを REDO ログバッファに記録する
func (rl *RedoLog) AppendRollback(trxId uint64) LSN {
	return rl.append(trxId, RedoRollback, page.PageId{}, nil)
}

// Flush はバッファの全レコードをディスクに書き込み、FlushedLSN を更新する
func (rl *RedoLog) Flush() error {
	if len(rl.buffer) == 0 {
		return nil
	}

	// ファイル末尾にシーク
	if _, err := rl.file.Seek(0, io.SeekEnd); err != nil {
		return err
	}

	// 全レコードを書き出し
	for _, record := range rl.buffer {
		if _, err := rl.file.Write(record.Serialize()); err != nil {
			return err
		}
	}

	// fsync でディスクへの書き込みを保証
	if err := rl.file.Sync(); err != nil {
		return err
	}

	// FlushedLSN を更新してヘッダーに書き込む
	rl.FlushedLSN = rl.buffer[len(rl.buffer)-1].LSN
	if err := rl.writeHeader(); err != nil {
		return err
	}

	rl.buffer = nil
	return nil
}

// ReadAll はディスクから全レコードを読み込む (リカバリ用)
func (rl *RedoLog) ReadAll() ([]RedoRecord, error) {
	stat, err := rl.file.Stat()
	if err != nil {
		return nil, err
	}

	fileSize := stat.Size()
	if fileSize <= redoLogHeaderSize {
		return nil, nil
	}

	// ヘッダー以降を読み込む
	data := make([]byte, fileSize-redoLogHeaderSize)
	if _, err := rl.file.ReadAt(data, redoLogHeaderSize); err != nil {
		return nil, err
	}

	// レコードを順次デシリアライズ
	var records []RedoRecord
	offset := 0
	for offset < len(data) {
		record, bytesRead, err := DeserializeRedoRecord(data[offset:])
		if err != nil {
			return records, err
		}
		records = append(records, record)
		offset += bytesRead
	}

	return records, nil
}

// Reset はファイルをクリアする (クリーンシャットダウン後に呼ぶ)
func (rl *RedoLog) Reset() error {
	if err := rl.file.Truncate(0); err != nil {
		return err
	}
	if _, err := rl.file.Seek(0, io.SeekStart); err != nil {
		return err
	}
	rl.FlushedLSN = 0
	rl.lsnGen = NewLSNGenerator(0)
	rl.buffer = nil
	return rl.writeHeader()
}

// append は新しい REDO レコードをバッファに追加し、対応する LSN を返す
func (rl *RedoLog) append(trxId uint64, recordType RedoRecordType, pageId page.PageId, data []byte) LSN {
	lsn := rl.lsnGen.AllocateLSN()
	rl.buffer = append(rl.buffer, RedoRecord{
		LSN:    lsn,
		TrxId:  trxId,
		Type:   recordType,
		PageId: pageId,
		Data:   data,
	})
	return lsn
}

// writeHeader はファイルヘッダーに FlushedLSN を書き込む
func (rl *RedoLog) writeHeader() error {
	header := make([]byte, redoLogHeaderSize)
	binary.BigEndian.PutUint32(header[0:4], uint32(rl.FlushedLSN))
	if _, err := rl.file.WriteAt(header, 0); err != nil {
		return err
	}
	return rl.file.Sync()
}
