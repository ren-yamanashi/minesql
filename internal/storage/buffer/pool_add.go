package buffer

import "github.com/ren-yamanashi/minesql/internal/storage/page"

// AddPage はバッファプールに新しいページを追加する
//
// バッファプールに空きがある場合は新しいページを追加し、空きがない場合は古いページを新しいページに置き換える
func (p *Pool) AddPage(pageId page.Id) (*Page, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.addPage(pageId)
}

// addPage はバッファプールに新しいページを追加する
func (p *Pool) addPage(pageId page.Id) (*Page, error) {
	// バッファプールに空きがある場合: 新しいバッファページを追加・ページテーブルを更新
	if len(p.pages) < p.maxPages {
		newBufPage, err := NewPage(pageId)
		if err != nil {
			return nil, err
		}
		p.pages = append(p.pages, *newBufPage)
		bufferId := id(len(p.pages) - 1)
		p.pageTable.add(pageId, bufferId)
		p.lru.access(bufferId)
		return &p.pages[bufferId], nil
	}

	// バッファプールに空きがない場合: ページを追い出す
	victimBufId := p.lru.evict()
	victimBufPage := &p.pages[victimBufId]

	if victimBufPage.isDirty {
		heapFile, err := p.heapFile(victimBufPage.pageId.FileId)
		if err != nil {
			p.lru.undoEvict(victimBufId)
			return nil, err
		}

		err = heapFile.Write(victimBufPage.pageId.PageNumber, victimBufPage.data.ToBytes())
		if err != nil {
			p.lru.undoEvict(victimBufId)
			return nil, err
		}

		p.flushList.delete(victimBufPage.pageId)
	}

	// 新しいページに置き換え (newPage を pageTable.update より先に実行し、失敗時の不整合を防ぐ)
	newBufPage, err := NewPage(pageId)
	if err != nil {
		p.lru.undoEvict(victimBufId)
		return nil, err
	}
	p.pageTable.update(victimBufPage.pageId, pageId, victimBufId)
	p.pages[victimBufId] = *newBufPage
	p.lru.access(victimBufId)
	return &p.pages[victimBufId], nil
}
