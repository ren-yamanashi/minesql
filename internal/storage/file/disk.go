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

// NewDisk は指定されたパスにあるヒープファイルを管理する Disk を生成する
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
		return nil, err
	}

	return &Disk{
		fileId:     fileId,
		heapFile:   file,
		nextPageId: page.NewPageId(fileId, page.PageNumber(fileInfo.Size()/page.PAGE_SIZE)),
	}, nil
}

// AllocatePage は新しいページ ID を採番する
func (d *Disk) AllocatePage() page.PageId {
	id := d.nextPageId
	d.nextPageId = page.NewPageId(d.fileId, d.nextPageId.PageNumber+1)
	return id
}

// ReadPageData は指定されたページ ID のページデータを data に読み込む (読み込んだデータは data に格納される)
//
// data の長さは PAGE_SIZE と等しい必要がある
func (d *Disk) ReadPageData(id page.PageId, data []byte) error {
	if len(data) != page.PAGE_SIZE {
		return page.ErrInvalidDataSize
	}
	if err := d.seek(id); err != nil {
		return err
	}
	// シークした位置から PAGE_SIZE バイト読み込む
	// 読み込んだデータは `data` に格納される
	_, err := io.ReadFull(d.heapFile, data) // data に PAGE_SIZE バイト読み込む (data の長さは PAGE_SIZE と等しいので ReadFull を使用すると PAGE_SIZE バイト読み込まれる)
	return err
}

// WritePageData は指定されたページ ID に対応するページに data の内容を書き込む
//
// data の長さは PAGE_SIZE と等しい必要がある
func (d *Disk) WritePageData(id page.PageId, data []byte) error {
	if len(data) != page.PAGE_SIZE {
		return page.ErrInvalidDataSize
	}
	if err := d.seek(id); err != nil {
		return err
	}
	// シークした位置から PAGE_SIZE バイト書き込む
	n, err := d.heapFile.Write(data)
	if err != nil {
		return err
	}
	// 書き込んだバイト数が PAGE_SIZE と等しいことを確認
	if n != page.PAGE_SIZE {
		return io.ErrShortWrite
	}
	return nil
}

// Sync はファイルをディスクに同期する (基本的にはプロセスの終了時に呼び出せば良い)
func (d *Disk) Sync() error {
	return d.heapFile.Sync()
}

// seek はページ ID で指定されたページの先頭にシークする
func (d *Disk) seek(id page.PageId) error {
	if id.FileId != d.fileId {
		return fmt.Errorf("invalid FileId: expected %d, got %d", d.fileId, id.FileId)
	}
	offset := page.PAGE_SIZE * uint64(id.PageNumber)
	_, err := d.heapFile.Seek(int64(offset), io.SeekStart)
	return err
}
