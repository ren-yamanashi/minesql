package disk

import "os"

const (
	PAGE_SIZE = 4096
)

type PageId uint64

type DiskManager struct {
	// ヒープファイルのファイルディスクリプタ
	heapFile *os.File
	// 採番するページ ID を決めるカウンタ
	nextPageId PageId
}

func NewDiskManager(file *os.File) (*DiskManager, error) {
	fileInfo, err := file.Stat()
	if err != nil {
		return nil, err
	}
	return &DiskManager{
		heapFile:   file,
		nextPageId: PageId(fileInfo.Size() / PAGE_SIZE),
	}, nil
}
