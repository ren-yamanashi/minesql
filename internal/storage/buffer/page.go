package buffer

import (
	"fmt"

	"github.com/ncw/directio"
	"github.com/ren-yamanashi/minesql/internal/storage/page"
	"github.com/ren-yamanashi/minesql/internal/storage/redo"
)

type Page struct {
	pageId                page.Id
	data                  *page.Page
	isDirty               bool
	pinCount              int
	latch                 *RWLatch
	modifyCount           uint64   // 書き込みのたびに増加する更新カウンタ
	oldestModificationLsn redo.Lsn // 最初にダーティ化した時の mtr 開始マーカー LSN。クリーン時は 0
	pool                  *Pool    // markDirty で flushList と isDirty を更新するためのバックポインタ
}

func (p *Page) PageId() page.Id     { return p.pageId }
func (p *Page) Data() *page.Page    { return p.data }
func (p *Page) Latch() *RWLatch     { return p.latch }
func (p *Page) ModifyCount() uint64 { return p.modifyCount }

// MarkModified はページ内容を変更した直後に呼び出し、isDirty/flushList/modifyCount を一括更新する
//   - 呼び出し側は X ラッチを保持中であることが前提
func (p *Page) MarkModified() {
	p.pool.markDirty(p)
}

// markDirtyFromLsn は「最初にダーティ化した時の LSN」を設定する
//   - oldestModificationLsn が 0 (= クリーン状態) のときのみ lsn を設定する
//   - 既に非 0 (= 既にダーティ) なら何もしない (最初の値を保持)
//   - 呼び出し側は X ラッチを保持中であることが前提
func (p *Page) markDirtyFromLsn(lsn redo.Lsn) {
	if p.oldestModificationLsn == 0 {
		p.oldestModificationLsn = lsn
	}
}

// OverwritePage はページ全体を src で上書きし、書き込み完了を通知する
//   - 呼び出し側は X ラッチを保持中であることが前提
//   - src の長さは page.Size でなければならない
func (p *Page) OverwritePage(src []byte) {
	if len(src) != page.Size {
		panic(fmt.Sprintf("buffer: OverwritePage requires src of len %d, got %d", page.Size, len(src)))
	}
	copy(p.data.Bytes(), src)
	p.MarkModified()
}

// WriteHeaderAt はページヘッダーの offset に src を書き込み、書き込み完了を通知する
//   - 呼び出し側は X ラッチを保持中であることが前提
//   - offset + len(src) がヘッダーサイズを超える場合は panic
func (p *Page) WriteHeaderAt(offset int, src []byte) {
	header := p.data.Header()
	if offset < 0 || offset+len(src) > len(header) {
		panic(fmt.Sprintf("buffer: WriteHeaderAt out of range: offset=%d, len=%d, headerSize=%d", offset, len(src), len(header)))
	}
	copy(header[offset:], src)
	p.MarkModified()
}

// WriteBodyAt はページボディの offset に src を書き込み、書き込み完了を通知する
//   - 呼び出し側は X ラッチを保持中であることが前提
//   - offset + len(src) がボディサイズを超える場合は panic
func (p *Page) WriteBodyAt(offset int, src []byte) {
	body := p.data.Body()
	if offset < 0 || offset+len(src) > len(body) {
		panic(fmt.Sprintf("buffer: WriteBodyAt out of range: offset=%d, len=%d, bodySize=%d", offset, len(src), len(body)))
	}
	copy(body[offset:], src)
	p.MarkModified()
}

func NewPage(pageId page.Id, pool *Pool) (*Page, error) {
	p, err := page.NewPage(directio.AlignedBlock(page.Size))
	if err != nil {
		return nil, err
	}
	return &Page{
		pageId:   pageId,
		data:     p,
		isDirty:  false,
		pinCount: 0,
		latch:    NewRWLatch(),
		pool:     pool,
	}, nil
}
