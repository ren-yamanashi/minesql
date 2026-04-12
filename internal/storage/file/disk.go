package file

import (
	"fmt"
	"io"
	"os"

	"minesql/internal/storage/page"

	"github.com/ncw/directio"
)

// Disk はディスク上のヒープファイルを管理する
type Disk struct {
	fileId     page.FileId // このディスクの FileId
	heapFile   *os.File    // ヒープファイルのファイルディスクリプタ
	nextPageId page.PageId // 次に採番するページ ID
}

// NewDisk は指定されたパスのヒープファイルを開き、Disk を生成する (ファイルが存在しない場合は新規作成する)
func NewDisk(fileId page.FileId, path string) (*Disk, error) {
	file, err := directio.OpenFile(
		path,
		os.O_RDWR|os.O_CREATE, // read-write モードで開き、存在しない場合は作成する (※ os.O_DIRECT は directio.OpenFile 内で設定される)
		0666,                  // パーミッション (rw-rw-rw-)([参照](https://web.tku.ac.jp/~densan/local/permission/permission.htm))
	)
	if err != nil {
		return nil, err
	}

	fileInfo, err := file.Stat()
	if err != nil {
		_ = file.Close()
		return nil, err
	}

	return &Disk{
		fileId:     fileId,
		heapFile:   file,
		nextPageId: page.NewPageId(fileId, page.PageNumber(fileInfo.Size()/page.PageSize)),
	}, nil
}

// AllocatePage は新しいページ ID を採番する
func (disk *Disk) AllocatePage() page.PageId {
	id := disk.nextPageId
	disk.nextPageId = page.NewPageId(disk.fileId, disk.nextPageId.PageNumber+1)
	return id
}

// ReadPageData は指定されたページ ID のページデータを data に読み込む (読み込んだデータは data に格納される)
//
// data の長さは PageSize と等しい必要がある
func (disk *Disk) ReadPageData(id page.PageId, data []byte) error {
	if len(data) != page.PageSize {
		return page.ErrInvalidDataSize
	}
	if err := disk.seek(id); err != nil {
		return err
	}
	// シークした位置から PageSize バイト読み込む
	// 読み込んだデータは `data` に格納される
	_, err := io.ReadFull(disk.heapFile, data)
	return err
}

// WritePageData は指定されたページ ID に対応するページに data の内容を書き込む
//
// data の長さは PageSize と等しい必要がある
func (disk *Disk) WritePageData(id page.PageId, data []byte) error {
	if len(data) != page.PageSize {
		return page.ErrInvalidDataSize
	}
	if err := disk.seek(id); err != nil {
		return err
	}
	// シークした位置から PageSize バイト書き込む
	n, err := disk.heapFile.Write(data)
	if err != nil {
		return err
	}
	// 書き込んだバイト数が PageSize と等しいことを確認
	if n != page.PageSize {
		return io.ErrShortWrite
	}
	return nil
}

// Sync はファイルをディスクに同期する
func (disk *Disk) Sync() error {
	return disk.heapFile.Sync()
}

// Close はヒープファイルのファイルディスクリプタを閉じる
func (disk *Disk) Close() error {
	return disk.heapFile.Close()
}

// seek はページ ID で指定されたページの先頭にシークする
func (disk *Disk) seek(id page.PageId) error {
	if id.FileId != disk.fileId {
		return fmt.Errorf("invalid FileId: expected %d, got %d", disk.fileId, id.FileId)
	}
	offset := page.PageSize * uint64(id.PageNumber)
	_, err := disk.heapFile.Seek(int64(offset), io.SeekStart)
	return err
}
