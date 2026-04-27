package file

import (
	"errors"
	"io"
	"os"

	"github.com/ncw/directio"
	"github.com/ren-yamanashi/minesql/internal/storage/page"
)

// DiskManager はヒープファイルを管理する
type DiskManager struct {
	fileId     page.FileId // 管理対象ファイルの FileId
	heapFile   *os.File    // ヒープファイルのファイルディスクリプタ
	nextPageId page.PageId // 次に採番する PageId
}

func NewDiskManager(fileId page.FileId, path string) (*DiskManager, error) {
	file, err := directio.OpenFile(
		path,
		os.O_RDWR|os.O_CREATE, // read-write モードで開き、存在しない場合は作成する (※ os.O_DIRECT は directio.OpenFile 内で設定される)
		0666,                  // パーミッション (rw-rw-rw-)(see: https://web.tku.ac.jp/~densan/local/permission/permission.htm)
	)
	if err != nil {
		return nil, err
	}

	fileInfo, err := file.Stat()
	if err != nil {
		return nil, errors.Join(err, file.Close())
	}

	return &DiskManager{
		fileId:     fileId,
		heapFile:   file,
		nextPageId: page.NewPageId(fileId, page.PageNumber(fileInfo.Size()/page.PageSize)),
	}, nil
}

// AllocatePageId は新しいページ ID を採番する
func (dm *DiskManager) AllocatePageId() page.PageId {
	id := dm.nextPageId
	dm.nextPageId = page.NewPageId(dm.fileId, dm.nextPageId.PageNumber+1)
	return id
}

// Read は指定された PageNumber のページデータを data に読み込む
// (読み込んだデータは data に格納される)
//   - pageNumber: 読み込み対象の PageNumber
//   - data: 読み込み先
func (dm *DiskManager) Read(pageNumber page.PageNumber, data []byte) error {
	if err := page.CheckPageSize(data); err != nil {
		return err
	}
	if err := dm.seek(pageNumber); err != nil {
		return err
	}
	// シークした位置から PageSize バイト読み込む
	_, err := io.ReadFull(dm.heapFile, data)
	return err
}

// Write は指定された PageNumber に対応するページに data を書き込む
//   - pageNumber: 書き込み対象の PageNumber
//   - data: 書き込むデータ
func (dm *DiskManager) Write(pageNumber page.PageNumber, data []byte) error {
	if err := page.CheckPageSize(data); err != nil {
		return err
	}
	if err := dm.seek(pageNumber); err != nil {
		return err
	}
	// シークした位置から書き込む
	n, err := dm.heapFile.Write(data)
	if err != nil {
		return err
	}
	if n != page.PageSize {
		return io.ErrShortWrite
	}
	return nil
}

// Sync はファイルをディスクに同期する
func (dm *DiskManager) Sync() error {
	return dm.heapFile.Sync()
}

// Close はヒープファイルのファイルディスクリプタを閉じる
func (dm *DiskManager) Close() error {
	return dm.heapFile.Close()
}

// seek は PageNumber で指定されたページの先頭にシークする
func (dm *DiskManager) seek(pageNumber page.PageNumber) error {
	offset := page.PageSize * pageNumber
	_, err := dm.heapFile.Seek(int64(offset), io.SeekStart)
	return err
}
