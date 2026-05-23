package buffer

import "github.com/ren-yamanashi/minesql/internal/storage/page"

// pageTable は PageId と BufferId の対応関係を管理するテーブル
type pageTable map[page.Id]id

func newPageTable() pageTable {
	return make(map[page.Id]id)
}

// bufferId は指定した pageId に対応するバッファ ID を返す
func (pt pageTable) bufferId(pageId page.Id) (id, bool) {
	bufId, exists := pt[pageId]
	return bufId, exists
}

// add はページテーブルにエントリを追加する
func (pt pageTable) add(pageId page.Id, bufferId id) {
	pt[pageId] = bufferId
}

// update はページテーブルを更新する
//   - evictPageId: 追い出されるページの PageId
//   - newPageId: 追加されるページの PageId
//   - bufferId: 追い出されるページが属する BufferId (新しいページも同じ bufferId になる)
func (pt pageTable) update(evictPageId, newPageId page.Id, bufferId id) {
	if oldBufferId, exists := pt[evictPageId]; exists && oldBufferId == bufferId {
		delete(pt, evictPageId)
	}
	pt[newPageId] = bufferId
}

// delete は pageId に対応するエントリをテーブルから削除する
func (pt pageTable) delete(pageId page.Id) {
	delete(pt, pageId)
}

// forEach は全エントリに対してコールバックを実行する
func (pt pageTable) forEach(fn func(pageId page.Id, bufferId id)) {
	for pageId, bufferId := range pt {
		fn(pageId, bufferId)
	}
}
