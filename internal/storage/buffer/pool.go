package buffer

import (
	"fmt"
	"sync"

	"github.com/ren-yamanashi/minesql/internal/storage/file"
	"github.com/ren-yamanashi/minesql/internal/storage/page"
)

type Pool struct {
	mu          sync.RWMutex
	files       map[page.FileId]*file.HeapFile
	pages       []Page
	pageTable   pageTable
	flushList   *flushList
	lru         *lru
	maxPages    int    // バッファプールの最大バッファページ数
	onAllPinned func() // 全ページが Pin/dirty で追い出し不可な状態を解消するための callback (e.g. ページクリーナーにフラッシュ依頼)
}

// NewPool はバッファプールを生成する
//   - size: バッファプール全体のバイトサイズ。最低 1 ページ確保される
//   - onAllPinned: 追い出し候補が全て Pin/dirty な状態を解消するための callback
func NewPool(size int, onAllPinned func()) *Pool {
	maxPages := 1
	if size > page.Size {
		maxPages = (size + page.Size - 1) / page.Size
	}
	return &Pool{
		files:       make(map[page.FileId]*file.HeapFile),
		pages:       make([]Page, 0, maxPages),
		pageTable:   newPageTable(),
		flushList:   newFlushList(),
		lru:         newLru(maxPages),
		maxPages:    maxPages,
		onAllPinned: onAllPinned,
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

	if !bufPage.isDirty {
		bufPage.isDirty = true
		p.flushList.add(pageId)
	}
	bufPage.pinCount++
	return bufPage, nil
}

// PageForRead は読み込み用のバッファページを取得する
func (p *Pool) PageForRead(pageId page.Id) (*Page, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	bufPage, err := p.page(pageId)
	if err != nil {
		return nil, err
	}
	bufPage.pinCount++
	return bufPage, nil
}

// Unpin は指定されたページの Pin カウントをデクリメントする
func (p *Pool) Unpin(pageId page.Id) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if bufferId, exists := p.pageTable.bufferId(pageId); exists {
		if p.pages[bufferId].pinCount > 0 {
			p.pages[bufferId].pinCount--
		}
	}
}

// AllocatePageId は指定された FileId に対して新しい PageId を割り当てる
func (p *Pool) AllocatePageId(fileId page.FileId) (page.Id, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	heapFile, err := p.heapFile(fileId)
	if err != nil {
		return page.InvalidId(), err
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

// FlushListPageCount はフラッシュリスト内のページ数を返す
func (p *Pool) FlushListPageCount() int {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.flushList.pageCount
}

// ForEachDirtyPage はフラッシュリスト内の全ダーティーページに対してコールバックを実行する
func (p *Pool) ForEachDirtyPage(fn func(pg *page.Page)) {
	p.mu.RLock()
	defer p.mu.RUnlock()

	p.flushList.forEach(func(pageId page.Id) {
		bufId, ok := p.pageTable.bufferId(pageId)
		if !ok {
			return
		}
		fn(p.pages[bufId].data)
	})
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
		bufPage := &p.pages[bufferId]
		p.lru.access(bufferId)
		return bufPage, nil
	}

	// ページがバッファプールにない場合
	bufPage, err := p.addPage(pageId)
	if err != nil {
		return nil, err
	}

	// ディスク上のファイルからページを読み込む
	heapFile, err := p.heapFile(pageId.FileId())
	if err != nil {
		p.pageTable.delete(pageId)
		return nil, err
	}
	err = heapFile.Read(pageId.PageNumber(), bufPage.data.Bytes())
	if err != nil {
		p.pageTable.delete(pageId)
		return nil, err
	}

	return bufPage, nil
}
