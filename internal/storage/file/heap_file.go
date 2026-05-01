package file

import (
	"errors"
	"io"
	"os"

	"github.com/ncw/directio"
	"github.com/ren-yamanashi/minesql/internal/storage/page"
)

type HeapFile struct {
	fileId     page.FileId // 管理対象ファイルの FileId
	file       *os.File    // ヒープファイルのファイルディスクリプタ
	nextPageId page.PageId // 次に採番する PageId
}

func NewHeapFile(fileId page.FileId, path string) (*HeapFile, error) {
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

	return &HeapFile{
		fileId:     fileId,
		file:       file,
		nextPageId: page.NewPageId(fileId, page.PageNumber(fileInfo.Size()/page.PageSize)),
	}, nil
}

// AllocatePageId は新しいページ ID を採番する
func (hf *HeapFile) AllocatePageId() page.PageId {
	id := hf.nextPageId
	hf.nextPageId = page.NewPageId(hf.fileId, hf.nextPageId.PageNumber+1)
	return id
}

// Read は指定された PageNumber のページデータを data に読み込む
// (読み込んだデータは data に格納される)
//   - pageNumber: 読み込み対象の PageNumber
//   - data: 読み込み先
func (hf *HeapFile) Read(pageNumber page.PageNumber, data []byte) error {
	if err := page.CheckPageSize(data); err != nil {
		return err
	}
	if err := hf.seek(pageNumber); err != nil {
		return err
	}
	// シークした位置から PageSize バイト読み込む
	_, err := io.ReadFull(hf.file, data)
	return err
}

// Write は指定された PageNumber に対応するページに data を書き込む
//   - pageNumber: 書き込み対象の PageNumber
//   - data: 書き込むデータ
func (hf *HeapFile) Write(pageNumber page.PageNumber, data []byte) error {
	if err := page.CheckPageSize(data); err != nil {
		return err
	}
	if err := hf.seek(pageNumber); err != nil {
		return err
	}
	// シークした位置から書き込む
	n, err := hf.file.Write(data)
	if err != nil {
		return err
	}
	if n != page.PageSize {
		return io.ErrShortWrite
	}
	return nil
}

// Sync はファイルをディスクに同期する
func (hf *HeapFile) Sync() error {
	return hf.file.Sync()
}

// Close はヒープファイルのファイルディスクリプタを閉じる
func (hf *HeapFile) Close() error {
	return hf.file.Close()
}

// seek は PageNumber で指定されたページの先頭にシークする
func (hf *HeapFile) seek(pageNumber page.PageNumber) error {
	offset := page.PageSize * pageNumber
	_, err := hf.file.Seek(int64(offset), io.SeekStart)
	return err
}
