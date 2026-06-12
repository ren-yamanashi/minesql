package buffer

import (
	"errors"
	"log"
	"time"

	"github.com/ren-yamanashi/minesql/internal/storage/page"
)

var ErrAllPagesUnevictable = errors.New("buffer: all pages are pinned or dirty")

const (
	// retryWarnIterations はリトライがこの回数を超えたら警告ログを出す閾値
	retryWarnIterations = 20

	// retryMaxIterations は追い出し候補が見つからないまま retry を続ける上限 (回復不能時のハングを防ぐフェイルセーフ)
	retryMaxIterations = 100

	// retryInterval は全ページ Pin/dirty 時のリトライ間隔
	retryInterval = 10 * time.Millisecond
)

// AddPage はバッファプールに新しいページを追加する
func (p *Pool) AddPage(pageId page.Id) (*Page, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.addPage(pageId)
}

// addPage はバッファプールに新しいページを追加する
func (p *Pool) addPage(pageId page.Id) (*Page, error) {
	// 再エントリ時の race (リトライ中に他 goroutine がキャッシュに追加した) を考慮して先頭で再確認
	if bufferId, exists := p.pageTable.bufferId(pageId); exists {
		return &p.pages[bufferId], nil
	}

	// バッファプールに空きがある場合: 新しいバッファページを追加・ページテーブルを更新
	if len(p.pages) < p.maxPages {
		newBufPage, err := NewPage(pageId, p)
		if err != nil {
			return nil, err
		}
		p.pages = append(p.pages, *newBufPage)
		bufferId := id(len(p.pages) - 1)
		p.pageTable.add(pageId, bufferId)
		p.lru.access(bufferId)
		return &p.pages[bufferId], nil
	}

	// バッファプールに空きがない場合: Pin == 0 かつ !isDirty なページを追い出す
	canEvict := func(bufId id) bool {
		return p.pages[bufId].pinCount == 0 && !p.pages[bufId].isDirty
	}

	victimBufId, err := p.evictWithRetry(pageId, canEvict)
	if err != nil {
		return nil, err
	}
	// リトライ中に他 goroutine が同じ pageId を追加済みなら、その bufferId が返るので早期 return
	if bufferId, exists := p.pageTable.bufferId(pageId); exists && bufferId == victimBufId {
		return &p.pages[bufferId], nil
	}

	victimBufPage := &p.pages[victimBufId]

	// 新しいページを先に確保し、失敗時に pageTable / pages が変更されていない状態を保つ
	newBufPage, err := NewPage(pageId, p)
	if err != nil {
		p.lru.undoEvict(victimBufId)
		return nil, err
	}
	p.pageTable.update(victimBufPage.pageId, pageId, victimBufId)
	p.pages[victimBufId] = *newBufPage
	p.lru.access(victimBufId)
	return &p.pages[victimBufId], nil
}

// evictWithRetry は追い出し候補が見つかるまで retry する
//
// - return:
// - pageId がリトライ中にキャッシュへ追加された場合はその bufferId
// - retryMaxIterations を超えても候補が見つからない場合は ErrAllPagesUnevictable
func (p *Pool) evictWithRetry(pageId page.Id, canEvict func(bufId id) bool) (id, error) {
	for iteration := 1; ; iteration++ {
		victimBufId, err := p.lru.evict(canEvict)
		if err == nil {
			return victimBufId, nil
		}
		if !errors.Is(err, ErrAllPagesUnevictable) {
			return 0, err
		}

		// onAllPinned が同期的に Pool のメソッド (FlushOldestPages 等) を呼んでも
		// self-deadlock しないよう、ロックを解放してから呼ぶ
		p.mu.Unlock()
		if p.onAllPinned != nil {
			p.onAllPinned()
		}
		_ = p.flushSinglePage()
		time.Sleep(retryInterval)
		p.mu.Lock()

		// 解放中に他 goroutine がキャッシュへ追加した可能性があるので確認
		if bufferId, exists := p.pageTable.bufferId(pageId); exists {
			return bufferId, nil
		}

		if iteration == retryWarnIterations+1 {
			log.Printf("buffer pool: difficult to find evictable pages (%d iterations)", iteration)
		}

		if iteration >= retryMaxIterations {
			return 0, ErrAllPagesUnevictable
		}
	}
}
