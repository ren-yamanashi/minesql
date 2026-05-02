package buffer

import "github.com/ren-yamanashi/minesql/internal/storage/page"

// PageTable は PageId と BufferId の対応関係を管理するテーブル
type PageTable map[page.PageId]BufferId

func NewPageTable() PageTable {
	return make(map[page.PageId]BufferId)
}

// GetBufferId は指定した pageId に対応するバッファページを返す
func (pt PageTable) GetBufferId(pageId page.PageId) (BufferId, bool) {
	bufId, exists := pt[pageId]
	return bufId, exists
}

// Add はページテーブルにエントリを追加する
func (pt PageTable) Add(pageId page.PageId, bufferId BufferId) {
	pt[pageId] = bufferId
}

// Update はページテーブルを更新する
//   - evictPageId: 追い出されるページの PageId
//   - newPageId: 追加されるページの PageId
//   - bufferId: 追い出されるページが属する BufferId (新しいページも同じ bufferId になる)
func (pt PageTable) Update(evictPageId, newPageId page.PageId, bufferId BufferId) {
	if oldBufferId, exists := pt[evictPageId]; exists && oldBufferId == bufferId {
		delete(pt, evictPageId)
	}
	pt[newPageId] = bufferId
}

// Delete は pageId に対応するエントリをテーブルから削除する
func (pt PageTable) Delete(pageId page.PageId) {
	delete(pt, pageId)
}

// ForEach は全エントリに対してコールバックを実行する
func (pt PageTable) ForEach(fn func(pageId page.PageId, bufferId BufferId)) {
	for pageId, bufferId := range pt {
		fn(pageId, bufferId)
	}
}
