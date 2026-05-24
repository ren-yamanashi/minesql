package redo

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/ncw/directio"
)

const (
	filename                      = "redo.log"
	tmpFilename                   = "redo.log.tmp"
	fileHeaderFlushedLsnOffset    = 0
	fileHeaderCheckpointLsnOffset = 4
	fileHeaderReservedAreaOffset  = 8
	fileHeaderSize                = 16
)

type file struct {
	osFile        *os.File // Redo ログファイルのファイルディスクリプタ
	filePath      string   // Redo ログファイルのパス
	flushedLsn    Lsn      // ディスクにフラッシュ済みの最大 LSN
	checkpointLsn Lsn      // チェックポイント LSN (この LSN 以前の Redo レコードは不要)
}

// newFile は redo.log ファイルを開く (存在しない場合は新規作成する)
func newFile(baseDir string) (*file, error) {
	filePath := filepath.Join(baseDir, filename)
	// read-write モードで開き、存在しない場合は作成する
	// (os.O_DIRECT は directio.OpenFile 内で設定される)
	osFile, err := directio.OpenFile(filePath, os.O_RDWR|os.O_CREATE, 0666)
	if err != nil {
		return nil, fmt.Errorf("redo: failed to open log file: %w", err)
	}

	f := &file{osFile: osFile, filePath: filePath}

	// ファイルヘッダーから FlushedLSN と CheckpointLSN を読み取る
	stat, err := osFile.Stat()
	if err != nil {
		return nil, errors.Join(err, osFile.Close())
	}

	// ファイルサイズがヘッダーサイズ以上ならヘッダーを読み取る
	if stat.Size() >= fileHeaderSize {
		header := make([]byte, fileHeaderSize)
		if _, err := osFile.ReadAt(header, 0); err != nil {
			return nil, errors.Join(err, osFile.Close())
		}
		flushedBytes := header[fileHeaderFlushedLsnOffset:fileHeaderCheckpointLsnOffset]
		f.flushedLsn = Lsn(binary.BigEndian.Uint32(flushedBytes))
		checkpointBytes := header[fileHeaderCheckpointLsnOffset:fileHeaderReservedAreaOffset]
		f.checkpointLsn = Lsn(binary.BigEndian.Uint32(checkpointBytes))
		return f, nil
	}

	// ファイルサイズがヘッダーサイズ未満なら新規ファイルとしてヘッダーを書き込む
	if err := f.writeHeader(); err != nil {
		return nil, errors.Join(err, osFile.Close())
	}
	return f, nil
}

// readRecords はディスクから、指定 LSN より大きい LSN を持つレコードを読み込む
func (f *file) readRecords(lsn Lsn) ([]Record, error) {
	size, err := f.size()
	if err != nil {
		return nil, err
	}
	if size <= fileHeaderSize {
		return nil, nil
	}

	body := make([]byte, size-fileHeaderSize)
	if _, err := f.osFile.ReadAt(body, fileHeaderSize); err != nil {
		return nil, err
	}

	var records []Record
	offset := 0
	for offset < len(body) {
		record, readBytesNum, err := DeserializeRecord(body[offset:])
		if err != nil {
			// 末尾の不完全レコードとデータ破損を区別する
			remaining := len(body) - offset
			if remaining >= recordHeaderSize {
				return nil, fmt.Errorf("redo: corrupted record at offset %d: %w", offset, err)
			}
			// ヘッダーサイズ未満の残りデータは、Write 途中のクラッシュで書きかけになったレコード
			// リカバリ時にこのデータは上書きされるため、無視して問題ない
			break
		}
		offset += readBytesNum
		if record.lsn <= lsn {
			continue
		}
		records = append(records, record)
	}
	return records, nil
}

// flushRecords はレコードをディスクに書き込み、FlushedLSN を更新する
func (f *file) flushRecords(records []Record) error {
	if len(records) == 0 {
		return nil
	}

	// 書き込み前のファイルサイズを記録 (部分書き込み時のロールバック用)
	originalSize, err := f.size()
	if err != nil {
		return err
	}

	if _, err := f.osFile.Seek(0, io.SeekEnd); err != nil {
		return err
	}

	for _, record := range records {
		if _, err := f.osFile.Write(record.Serialize()); err != nil {
			// 部分書き込みをロールバック
			_ = f.osFile.Truncate(originalSize)
			return err
		}
	}

	if err := f.osFile.Sync(); err != nil {
		_ = f.osFile.Truncate(originalSize)
		return err
	}

	// Flushed LSN を更新してヘッダーに書き込み
	prevFlushedLsn := f.flushedLsn
	f.flushedLsn = records[len(records)-1].lsn
	if err := f.writeHeader(); err != nil {
		f.flushedLsn = prevFlushedLsn
		return err
	}
	return nil
}

// setCheckpointLsn はチェックポイント LSN を更新し、ヘッダーに書き込む
func (f *file) setCheckpointLsn(lsn Lsn) error {
	prev := f.checkpointLsn
	f.checkpointLsn = lsn
	if err := f.writeHeader(); err != nil {
		f.checkpointLsn = prev
		return err
	}
	return nil
}

// truncateBefore は指定 LSN 以前のレコードをファイルから切り詰める
func (f *file) truncateBefore(lsn Lsn) error {
	records, err := f.readRecords(Lsn(0))
	if err != nil {
		return err
	}

	// 残すべきレコードと最終 LSN を決定
	var lastLsn Lsn
	var remaining []Record
	for _, rec := range records {
		if rec.lsn <= lsn {
			continue
		}
		remaining = append(remaining, rec)
		lastLsn = rec.lsn
	}

	// 一時ファイルにヘッダー + 残レコードを書き込む
	tmpPath := filepath.Join(filepath.Dir(f.filePath), tmpFilename)
	if err := f.writeTmpFile(tmpPath, max(lastLsn, lsn), remaining); err != nil {
		_ = os.Remove(tmpPath)
		return err
	}

	// 現在のファイルを閉じて置換
	// 原子性を確保するため、一時ファイルに書き出してから置換する
	if err := f.osFile.Close(); err != nil {
		_ = os.Remove(tmpPath)
		return err
	}
	if err := os.Rename(tmpPath, f.filePath); err != nil {
		// 置換失敗時: 元ファイルを再オープン
		f.osFile, _ = directio.OpenFile(f.filePath, os.O_RDWR, 0666)
		_ = os.Remove(tmpPath)
		return err
	}

	// 新しいファイルを開き直す
	osFile, err := directio.OpenFile(f.filePath, os.O_RDWR, 0666)
	if err != nil {
		return fmt.Errorf("redo: failed to reopen log file after truncate: %w", err)
	}
	f.osFile = osFile
	f.flushedLsn = max(lastLsn, lsn)
	return nil
}

// writeTmpFile は一時ファイルにヘッダーとレコードを書き込む
func (f *file) writeTmpFile(tmpPath string, flushedLsn Lsn, records []Record) (retErr error) {
	tmpFile, err := os.Create(tmpPath) //nolint:gosec // 内部で生成したパスを使用
	if err != nil {
		return err
	}
	defer func() {
		if closeErr := tmpFile.Close(); closeErr != nil && retErr == nil {
			retErr = closeErr
		}
	}()

	// ヘッダーを書き込み
	header := make([]byte, fileHeaderSize)
	binary.BigEndian.PutUint32(header[fileHeaderFlushedLsnOffset:fileHeaderCheckpointLsnOffset], uint32(flushedLsn))
	binary.BigEndian.PutUint32(header[fileHeaderCheckpointLsnOffset:fileHeaderReservedAreaOffset], uint32(f.checkpointLsn))
	if _, err := tmpFile.Write(header); err != nil {
		return err
	}

	// レコードを書き込み
	for _, rec := range records {
		if _, err := tmpFile.Write(rec.Serialize()); err != nil {
			return err
		}
	}

	return tmpFile.Sync()
}

// clear は Redo ログファイルをクリアする
func (f *file) clear() error {
	if err := f.osFile.Truncate(0); err != nil {
		return err
	}
	if _, err := f.osFile.Seek(0, io.SeekStart); err != nil {
		return err
	}
	f.flushedLsn = 0
	f.checkpointLsn = 0
	return f.writeHeader()
}

// close は Redo ログファイルを閉じる
func (f *file) close() error {
	return f.osFile.Close()
}

// size は Redo ログファイルの現在のサイズ (バイト数) を返す
func (f *file) size() (int64, error) {
	stat, err := f.osFile.Stat()
	if err != nil {
		return 0, err
	}
	return stat.Size(), nil
}

// writeHeader はファイルヘッダーに FlushedLSN と CheckpointLSN を書き込む
func (f *file) writeHeader() error {
	header := make([]byte, fileHeaderSize)
	flushedSlice := header[fileHeaderFlushedLsnOffset:fileHeaderCheckpointLsnOffset]
	binary.BigEndian.PutUint32(flushedSlice, uint32(f.flushedLsn))
	checkpointSlice := header[fileHeaderCheckpointLsnOffset:fileHeaderReservedAreaOffset]
	binary.BigEndian.PutUint32(checkpointSlice, uint32(f.checkpointLsn))
	if _, err := f.osFile.WriteAt(header, 0); err != nil {
		return err
	}
	return f.osFile.Sync()
}
