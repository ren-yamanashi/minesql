package buffer

import "github.com/ren-yamanashi/minesql/internal/storage/page"

// pageTable は PageId と BufferId の対応関係を管理するテーブル
type pageTable map[page.PageId]BufferId

func newPageTable() pageTable {
	return make(map[page.PageId]BufferId)
}

// getBufferId は指定した pageId に対応するバッファページを返す
func (pt pageTable) getBufferId(pageId page.PageId) (BufferId, bool) {
	bufId, exists := pt[pageId]
	return bufId, exists
}

// add はページテーブルにエントリを追加する
func (pt pageTable) add(pageId page.PageId, bufferId BufferId) {
	pt[pageId] = bufferId
}

// update はページテーブルを更新する
//   - evictPageId: 追い出されるページの PageId
//   - newPageId: 追加されるページの PageId
//   - bufferId: 追い出されるページが属する BufferId (新しいページも同じ bufferId になる)
func (pt pageTable) update(evictPageId, newPageId page.PageId, bufferId BufferId) {
	if oldBufferId, exists := pt[evictPageId]; exists && oldBufferId == bufferId {
		delete(pt, evictPageId)
	}
	pt[newPageId] = bufferId
}

// delete は pageId に対応するエントリをテーブルから削除する
func (pt pageTable) delete(pageId page.PageId) {
	delete(pt, pageId)
}

// forEach は全エントリに対してコールバックを実行する
func (pt pageTable) forEach(fn func(pageId page.PageId, bufferId BufferId)) {
	for pageId, bufferId := range pt {
		fn(pageId, bufferId)
	}
}
