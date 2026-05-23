package buffer

import (
	"fmt"
	"sync"

	"github.com/ren-yamanashi/minesql/internal/storage/file"
	"github.com/ren-yamanashi/minesql/internal/storage/page"
)

type Pool struct {
	mu        sync.RWMutex
	files     map[page.FileId]*file.HeapFile
	pages     []Page
	pageTable pageTable
	flushList *flushList
	lru       *lru
	maxPages  int // バッファプールの最大バッファページ数
}

func NewPool(size int) *Pool {
	var maxNumOfPage int
	if size <= page.Size {
		maxNumOfPage = 1
	} else {
		maxNumOfPage = (size / page.Size) + 1
	}
	return &Pool{
		files:     make(map[page.FileId]*file.HeapFile),
		pages:     make([]Page, 0, maxNumOfPage),
		pageTable: newPageTable(),
		flushList: newFlushList(),
		lru:       newLru(maxNumOfPage),
		maxPages:  maxNumOfPage,
	}
}

// PageForWrite は書き込み用のバッファページを取得する
func (p *Pool) PageForWrite(pageId page.Id) (*Page, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	bufPage, err := p.page(pageId)
	if err != nil {
		return nil, err
	}

	// 書き込み用なのでダーティーページとして扱う
	if !bufPage.isDirty {
		bufPage.isDirty = true
		p.flushList.add(pageId)
	}
	return bufPage, nil
}

// PageForRead は読み込み用のバッファページを取得する
func (p *Pool) PageForRead(pageId page.Id) (*Page, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.page(pageId)
}

// UnRefPage は指定されたページの参照を解除し、優先的に追い出されるようにする
func (p *Pool) UnRefPage(pageId page.Id) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if bufferId, exists := p.pageTable.bufferId(pageId); exists {
		p.lru.delete(bufferId)
	}
}

// AllocatePageId は指定された FileId に対して新しい PageId を割り当てる
func (p *Pool) AllocatePageId(fileId page.FileId) (page.Id, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	heapFile, err := p.heapFile(fileId)
	if err != nil {
		return page.InvalidId, err
	}
	return heapFile.AllocatePageId()
}

// RegisterHeapFile は BufferPool に HeapFile を登録する
//   - fileId: 登録する HeapFile に対応する FileId
//   - heapFile: 登録する HeapFile
func (p *Pool) RegisterHeapFile(fileId page.FileId, heapFile *file.HeapFile) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.files[fileId] = heapFile
}

// MaxPages はバッファプールの最大バッファページ数を返す
func (p *Pool) MaxPages() int {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.maxPages
}

// heapFile は指定された FileId に対応する HeapFile を取得する
func (p *Pool) heapFile(fileId page.FileId) (*file.HeapFile, error) {
	heapFile, ok := p.files[fileId]
	if !ok {
		return nil, fmt.Errorf("heap file for FileId %d not found", fileId)
	}
	return heapFile, nil
}

// page は指定されたページをバッファプールから取得する
func (p *Pool) page(pageId page.Id) (*Page, error) {
	// ページがバッファプールにある場合
	if bufferId, exists := p.pageTable.bufferId(pageId); exists {
		bufferPage := &p.pages[bufferId]
		p.lru.access(bufferId)
		return bufferPage, nil
	}

	// ページがバッファプールにない場合
	bufPage, err := p.addPage(pageId)
	if err != nil {
		return nil, err
	}

	// ディスク上のファイルからページを読み込む
	heapFile, err := p.heapFile(pageId.FileId)
	if err != nil {
		return nil, err
	}
	err = heapFile.Read(pageId.PageNumber, bufPage.Page.ToBytes())
	if err != nil {
		return nil, err
	}
	bufPage.PageId = pageId
	bufPage.isDirty = false

	return bufPage, nil
}
