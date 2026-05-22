package buffer

import (
	"github.com/ren-yamanashi/minesql/internal/storage/page"
)

type flushListNode struct {
	pageId page.PageId // このノードが表すページの PageId
	prev   *flushListNode
	next   *flushListNode
}

// flushList はダーティーページをダーティーになった順に管理する双方向リンクリスト
type flushList struct {
	NumOfPage int            // リスト内のページ数
	Head      *flushListNode // 最も古いダーティーページ
	Tail      *flushListNode // 最も新しいダーティーページ
	nodeMap   map[page.PageId]*flushListNode
}

func newFlushList() *flushList {
	return &flushList{
		nodeMap: make(map[page.PageId]*flushListNode),
	}
}

// add はページをフラッシュリストの末尾に追加する
func (fl *flushList) add(pageId page.PageId) {
	if _, exists := fl.nodeMap[pageId]; exists {
		return
	}

	node := &flushListNode{pageId: pageId}
	fl.nodeMap[pageId] = node

	if fl.Tail == nil {
		fl.Head = node
		fl.Tail = node
	} else {
		node.prev = fl.Tail
		fl.Tail.next = node
		fl.Tail = node
	}
	fl.NumOfPage++
}

// delete はページをフラッシュリストから削除する
func (fl *flushList) delete(pageId page.PageId) {
	node, exists := fl.nodeMap[pageId]
	if !exists {
		return
	}

	if node.prev != nil {
		node.prev.next = node.next
	} else {
		fl.Head = node.next
	}

	if node.next != nil {
		node.next.prev = node.prev
	} else {
		fl.Tail = node.prev
	}

	delete(fl.nodeMap, pageId)
	fl.NumOfPage--
}

// clear はフラッシュリスト全体をクリアする
func (fl *flushList) clear() {
	fl.Head = nil
	fl.Tail = nil
	fl.NumOfPage = 0
	fl.nodeMap = make(map[page.PageId]*flushListNode)
}

// oldestPageIds は先頭 (最も古い) から n 件の PageId を返す
func (fl *flushList) oldestPageIds(n int) []page.PageId {
	result := make([]page.PageId, 0, n)
	node := fl.Head
	for node != nil && len(result) < n {
		result = append(result, node.pageId)
		node = node.next
	}
	return result
}
