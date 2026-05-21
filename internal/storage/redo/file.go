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

type File struct {
	file          *os.File // Redo ログファイルのファイルディスクリプタ
	flushedLsn    Lsn      // ディスクにフラッシュ済みの最大 LSN
	checkPointLsn Lsn      // チェックポイント LSN (この LSN 以前の Redo レコードは不要)
}

func newFile() (*File, error) {
	filePath := filepath.Join(config.BaseDir, filename)
	// read-write モードで開き、存在しない場合は作成する
	// (os.O_DIRECT は directio.OpenFile 内で設定される)
	file, err := directio.OpenFile(filePath, os.O_RDWR|os.O_CREATE, 0666)
	if err != nil {
		return nil, fmt.Errorf("failed to open redo log file: %w", err)
	}

	f := &File{file: file}

	// ファイルヘッダーから FlushedLSN と CheckPointLSN を読み取る
	stat, err := file.Stat()
	if err != nil {
		return nil, errors.Join(err, file.Close())
	}

	// ファイルサイズがヘッダーサイズ以上ならヘッダーを読み取る
	if stat.Size() >= fileHeaderSize {
		header := make([]byte, fileHeaderSize)
		if _, err := file.ReadAt(header, 0); err != nil {
			return nil, errors.Join(err, file.Close())
		}
		f.flushedLsn = Lsn(binary.BigEndian.Uint32(header[fileHeaderFlushedLsnOffset:fileHeaderCheckpointLsnOffset]))
		f.checkPointLsn = Lsn(binary.BigEndian.Uint32(header[fileHeaderCheckpointLsnOffset:fileHeaderReservedAreaOffset]))
		return f, nil
	}

	// ファイルサイズがヘッダーサイズ未満なら新規ファイルとしてヘッダーを書き込む
	if err := f.writeHeader(); err != nil {
		return nil, errors.Join(err, file.Close())
	}
	return f, nil
}

// readRecords はディスクから、指定 LSN より大きい LSN を持つレコードを読み込む
func (f *File) readRecords(lsn Lsn) ([]Record, error) {
	size, err := f.size()
	if err != nil {
		return nil, err
	}
	if size <= fileHeaderSize {
		return nil, nil
	}

	body := make([]byte, size-fileHeaderSize)
	if _, err := f.file.ReadAt(body, fileHeaderSize); err != nil {
		return nil, err
	}

	var records []Record
	offset := 0
	for offset < len(body) {
		record, readBytesNum, err := DeserializeRecord(body[offset:])
		if err != nil {
			return nil, err
		}
		offset += readBytesNum
		if record.Lsn <= lsn {
			continue
		}
		records = append(records, record)
	}
	return records, nil
}

// flushRecords はレコードをディスクに書き込み、FlushedLSN を更新する
func (f *File) flushRecords(records []Record) error {
	if _, err := f.file.Seek(0, io.SeekEnd); err != nil {
		return err
	}

	for _, record := range records {
		if _, err := f.file.Write(record.Serialize()); err != nil {
			return err
		}
	}

	if err := f.file.Sync(); err != nil {
		return err
	}

	// Flushed LSN を更新してヘッダーに書き込み
	f.flushedLsn = records[len(records)-1].Lsn
	return f.writeHeader()
}

// TruncateBefore は指定 LSN 以前のレコードをファイルから切り詰める
func (f *File) truncateBefore(lsn Lsn) error {
	records, err := f.readRecords(Lsn(0))
	if err != nil {
		return err
	}

	// ファイルをヘッダーだけの状態にする
	if err := f.file.Truncate(fileHeaderSize); err != nil {
		return err
	}
	if _, err := f.file.Seek(fileHeaderSize, io.SeekStart); err != nil {
		return err
	}

	// 指定 LSN より大きいレコードだけ書き直す
	for _, rec := range records {
		if rec.Lsn <= lsn {
			continue
		}
		if _, err := f.file.Write(rec.Serialize()); err != nil {
			return err
		}
	}
	return f.file.Sync()
}

// clear は Redo ログファイルをクリアする
func (f *File) clear() error {
	if err := f.file.Truncate(0); err != nil {
		return err
	}
	if _, err := f.file.Seek(0, io.SeekStart); err != nil {
		return err
	}
	f.flushedLsn = 0
	f.checkPointLsn = 0
	return f.writeHeader()
}

// size は Redo ログファイルの現在のサイズ (バイト数) を返す
func (f *File) size() (int64, error) {
	stat, err := f.file.Stat()
	if err != nil {
		return 0, err
	}
	return stat.Size(), nil
}

// writeHeader はファイルヘッダーに FlushedLSN と CheckpointLSN を書き込む
func (f *File) writeHeader() error {
	header := make([]byte, fileHeaderSize)
	binary.BigEndian.PutUint32(header[fileHeaderFlushedLsnOffset:fileHeaderCheckpointLsnOffset], uint32(f.flushedLsn))
	binary.BigEndian.PutUint32(header[fileHeaderCheckpointLsnOffset:fileHeaderReservedAreaOffset], uint32(f.checkPointLsn))
	if _, err := f.file.WriteAt(header, 0); err != nil {
		return err
	}
	return f.file.Sync()
}
