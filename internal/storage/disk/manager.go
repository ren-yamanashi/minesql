package disk

import (
	"fmt"
	"io"
	"minesql/internal/storage/page"
	"os"
)

type DiskManager struct {
	// このディスクマネージャの FileId
	fileId page.FileId
	// ヒープファイルのファイルディスクリプタ
	heapFile *os.File
	// 次に採番するページ ID
	nextPageId page.PageId
}

// 指定されたパスにあるディスク上のヒープファイルを管理する DiskManager を生成する
func NewDiskManager(fileId page.FileId, path string) (*DiskManager, error) {
	file, err := os.OpenFile(
		path,
		os.O_RDWR|os.O_CREATE, // read-write モードで開き、存在しない場合は作成する
		0666,                  // パーミッション (rw-rw-rw-)([参照](https://web.tku.ac.jp/~densan/local/permission/permission.htm))
	)
	if err != nil {
		return nil, err
	}

	fileInfo, err := file.Stat()
	if err != nil {
		return nil, err
	}

	return &DiskManager{
		fileId:     fileId,
		heapFile:   file,
		nextPageId: page.NewPageId(fileId, page.PageNumber(fileInfo.Size()/page.PAGE_SIZE)),
	}, nil
}

// 指定されたページ ID のページデータを data に読み込む (読み込んだデータは data に格納される)
// data の長さは PAGE_SIZE と等しい必要がある
func (disk *DiskManager) ReadPageData(id page.PageId, data []byte) error {
	if len(data) != page.PAGE_SIZE {
		return page.ErrInvalidDataSize
	}

	if id.FileId != disk.fileId {
		return fmt.Errorf("invalid FileId: expected %d, got %d", disk.fileId, id.FileId)
	}

	err := disk.seek(id.PageNumber)
	if err != nil {
		return err
	}

	_, err = io.ReadFull(disk.heapFile, data) // data に PAGE_SIZE バイト読み込む (data の長さは PAGE_SIZE と等しいので ReadFull を使用すると PAGE_SIZE バイト読み込まれる)
	if err != nil {
		return err
	}
	return nil
}

// 指定されたページ ID に対応するページに data の内容を書き込む
// data の長さは PAGE_SIZE と等しい必要がある
func (disk *DiskManager) WritePageData(id page.PageId, data []byte) error {
	if len(data) != page.PAGE_SIZE {
		return page.ErrInvalidDataSize
	}

	if id.FileId != disk.fileId {
		return fmt.Errorf("invalid FileId: expected %d, got %d", disk.fileId, id.FileId)
	}

	err := disk.seek(id.PageNumber)
	if err != nil {
		return err
	}

	n, err := disk.heapFile.Write(data)
	if err != nil {
		return err
	}
	if n != page.PAGE_SIZE {
		return io.ErrShortWrite
	}
	return nil
}

// 新しいページ ID を採番する
func (disk *DiskManager) AllocatePage() page.PageId {
	id := disk.nextPageId
	// 次のページ番号をインクリメント
	disk.nextPageId = page.NewPageId(disk.fileId, disk.nextPageId.PageNumber+1)
	return id
}

// 指定されたページ番号に対応するページの先頭にシークする
func (disk *DiskManager) seek(pageNumber page.PageNumber) error {
	offset := page.PAGE_SIZE * uint64(pageNumber)             // 開始位置を計算
	_, err := disk.heapFile.Seek(int64(offset), io.SeekStart) // ファイルの先頭から offset バイト移動
	if err != nil {
		return err
	}
	return nil
}
