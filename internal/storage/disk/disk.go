package disk

import (
	"fmt"
	"io"
	"minesql/internal/storage/page"
	"os"

	"github.com/ncw/directio"
)

// Disk はディスク上のヒープファイルを管理する
type Disk struct {
	fileId     page.FileId // このディスクマネージャの FileId
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
func (disk *Disk) AllocatePage() page.PageId {
	id := disk.nextPageId
	// 次のページ番号をインクリメント
	disk.nextPageId = page.NewPageId(disk.fileId, disk.nextPageId.PageNumber+1)
	return id
}

// ReadPageData は指定されたページ ID のページデータを data に読み込む (読み込んだデータは data に格納される)
// data の長さは PAGE_SIZE と等しい必要がある
func (disk *Disk) ReadPageData(id page.PageId, data []byte) error {
	if len(data) != page.PAGE_SIZE {
		return page.ErrInvalidDataSize
	}

	if id.FileId != disk.fileId {
		return fmt.Errorf("invalid FileId: expected %d, got %d", disk.fileId, id.FileId)
	}

	// 指定されたページ番号に対応するページの先頭にシークする
	err := disk.seek(id.PageNumber)
	if err != nil {
		return err
	}

	// シークした位置から PAGE_SIZE バイト読み込む
	// 読み込んだデータは `data` に格納される
	_, err = io.ReadFull(disk.heapFile, data) // data に PAGE_SIZE バイト読み込む (data の長さは PAGE_SIZE と等しいので ReadFull を使用すると PAGE_SIZE バイト読み込まれる)
	if err != nil {
		return err
	}
	return nil
}

// WritePageData は指定されたページ ID に対応するページに data の内容を書き込む
// data の長さは PAGE_SIZE と等しい必要がある
func (disk *Disk) WritePageData(id page.PageId, data []byte) error {
	if len(data) != page.PAGE_SIZE {
		return page.ErrInvalidDataSize
	}

	if id.FileId != disk.fileId {
		return fmt.Errorf("invalid FileId: expected %d, got %d", disk.fileId, id.FileId)
	}

	// 指定されたページ番号に対応するページの先頭にシークする
	err := disk.seek(id.PageNumber)
	if err != nil {
		return err
	}

	// シークした位置から PAGE_SIZE バイト書き込む
	n, err := disk.heapFile.Write(data)
	if err != nil {
		return err
	}

	// 書き込んだバイト数が PAGE_SIZE と等しいことを確認
	if n != page.PAGE_SIZE {
		return io.ErrShortWrite
	}

	return nil
}

// Sync はファイルをディスクに同期する
// `file.Write(data)` は OS のキャッシュにデータを書き込むだけで、必ずしもディスクに書き込まれるとは限らないため、明示的に同期を行う必要がある
// 基本的にはプロセスの終了時に呼び出せば良い
// 参考: https://www.sobyte.net/post/2022-01/golang-defer-file-close/
func (disk *Disk) Sync() error {
	return disk.heapFile.Sync()
}

// seek は指定されたページ番号に対応するページの先頭にシークする
func (disk *Disk) seek(pageNumber page.PageNumber) error {
	offset := page.PAGE_SIZE * uint64(pageNumber)             // 開始位置を計算
	_, err := disk.heapFile.Seek(int64(offset), io.SeekStart) // ファイルの先頭から offset バイト移動
	if err != nil {
		return err
	}
	return nil
}
