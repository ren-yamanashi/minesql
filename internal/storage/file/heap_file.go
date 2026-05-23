package file

import (
	"errors"
	"fmt"
	"io"
	"os"

	"github.com/ncw/directio"
	"github.com/ren-yamanashi/minesql/internal/storage/page"
)

type HeapFile struct {
	fileId     page.FileId // 管理対象ファイルの FileId
	file       *os.File    // ヒープファイルのファイルディスクリプタ
	nextPageId page.Id     // 次に採番する PageId
}

func NewHeapFile(fileId page.FileId, path string) (heapFile *HeapFile, retErr error) {
	file, err := directio.OpenFile(
		path,
		os.O_RDWR|os.O_CREATE, // read-write モードで開き、存在しない場合は作成する (※ os.O_DIRECT は directio.OpenFile 内で設定される)
		0666,                  // パーミッション (rw-rw-rw-)(see: https://web.tku.ac.jp/~densan/local/permission/permission.htm)
	)
	if err != nil {
		return nil, err
	}
	defer func() {
		if retErr != nil {
			retErr = errors.Join(retErr, file.Close())
		}
	}()

	fileInfo, err := file.Stat()
	if err != nil {
		return nil, err
	}

	return &HeapFile{
		fileId:     fileId,
		file:       file,
		nextPageId: page.NewId(fileId, page.PageNumber(fileInfo.Size()/page.Size)),
	}, nil
}

// AllocatePageId は新しいページ ID を採番する
//
// PageNumber が上限に達している場合はエラーを返す
func (hf *HeapFile) AllocatePageId() (page.Id, error) {
	if hf.nextPageId.PageNumber >= page.MaxPageNumber {
		return page.InvalidId, fmt.Errorf("file %d: page number limit reached", hf.fileId)
	}
	id := hf.nextPageId
	hf.nextPageId = page.NewId(hf.fileId, hf.nextPageId.PageNumber+1)
	return id, nil
}

// Read は指定された PageNumber のページデータを data に読み込む
//   - pageNumber: 読み込み対象の PageNumber
//   - data: 読み込み先
//   - return: ページが存在しない場合は io.EOF、途中までしか読めなかった場合は io.ErrUnexpectedEOF
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
	if n != page.Size {
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
	offset := int64(page.Size) * int64(pageNumber)
	_, err := hf.file.Seek(offset, io.SeekStart)
	return err
}
