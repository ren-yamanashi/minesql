package redo

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/ncw/directio"
	"github.com/ren-yamanashi/minesql/internal/storage/config"
)

const (
	filename                      = "redo.log"
	fileHeaderFlushedLsnOffset    = 0
	fileHeaderCheckpointLsnOffset = 4
	fileHeaderReservedAreaOffset  = 8
	fileHeaderSize                = 16
)

type file struct {
	osFile        *os.File // Redo ログファイルのファイルディスクリプタ
	flushedLsn    Lsn      // ディスクにフラッシュ済みの最大 LSN
	checkpointLsn Lsn      // チェックポイント LSN (この LSN 以前の Redo レコードは不要)
}

// newFile は redo.log ファイルを開く (存在しない場合は新規作成する)
func newFile() (*file, error) {
	filePath := filepath.Join(config.BaseDir, filename)
	// read-write モードで開き、存在しない場合は作成する
	// (os.O_DIRECT は directio.OpenFile 内で設定される)
	osFile, err := directio.OpenFile(filePath, os.O_RDWR|os.O_CREATE, 0666)
	if err != nil {
		return nil, fmt.Errorf("redo: failed to open log file: %w", err)
	}

	f := &file{osFile: osFile}

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

	records := []Record{}
	offset := 0
	for offset < len(body) {
		record, readBytesNum, err := deserializeRecord(body[offset:])
		if err != nil {
			break // 末尾の不完全レコードはクラッシュ時に発生しうるため無視する
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

	if _, err := f.osFile.Seek(0, io.SeekEnd); err != nil {
		return err
	}

	for _, record := range records {
		if _, err := f.osFile.Write(record.Serialize()); err != nil {
			return err
		}
	}

	if err := f.osFile.Sync(); err != nil {
		return err
	}

	// Flushed LSN を更新してヘッダーに書き込み
	f.flushedLsn = records[len(records)-1].lsn
	return f.writeHeader()
}

// setCheckpointLsn はチェックポイント LSN を更新し、ヘッダーに書き込む
func (f *file) setCheckpointLsn(lsn Lsn) error {
	f.checkpointLsn = lsn
	return f.writeHeader()
}

// truncateBefore は指定 LSN 以前のレコードをファイルから切り詰める
func (f *file) truncateBefore(lsn Lsn) error {
	records, err := f.readRecords(Lsn(0))
	if err != nil {
		return err
	}

	// ファイルをヘッダーだけの状態にする
	if err := f.osFile.Truncate(fileHeaderSize); err != nil {
		return err
	}
	if _, err := f.osFile.Seek(fileHeaderSize, io.SeekStart); err != nil {
		return err
	}

	// 指定 LSN より大きいレコードだけ書き直す
	var lastLsn Lsn
	for _, rec := range records {
		if rec.lsn <= lsn {
			continue
		}
		if _, err := f.osFile.Write(rec.Serialize()); err != nil {
			return err
		}
		lastLsn = rec.lsn
	}

	if err := f.osFile.Sync(); err != nil {
		return err
	}

	// flushedLsn を更新してヘッダーに書き込む
	// 全レコードが切り詰められた場合でも、指定 LSN までは処理済みなので lsn を下限とする
	f.flushedLsn = max(lastLsn, lsn)
	return f.writeHeader()
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
