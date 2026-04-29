package buffer

import (
	"github.com/ren-yamanashi/minesql/internal/storage/page"
)

type flushListNode struct {
	pageId page.PageId // このノードが表すページの PageId
	prev   *flushListNode
	next   *flushListNode
}

// FlushList はダーティーページをダーティーになった順に管理する双方向リンクリスト
type FlushList struct {
	NumOfPage uint32         // リスト内のページ数
	head      *flushListNode // 最も古いダーティーページ
	tail      *flushListNode // 最も新しいダーティーページ
	nodeMap   map[page.PageId]*flushListNode
}

func NewFlushList() *FlushList {
	return &FlushList{
		nodeMap: make(map[page.PageId]*flushListNode),
	}
}

// Add はページをフラッシュリストの末尾に追加する
func (fl *FlushList) Add(pageId page.PageId) {
	if _, exists := fl.nodeMap[pageId]; exists {
		return
	}

	node := &flushListNode{pageId: pageId}
	fl.nodeMap[pageId] = node

	if fl.tail == nil {
		fl.head = node
		fl.tail = node
	} else {
		node.prev = fl.tail
		fl.tail.next = node
		fl.tail = node
	}
	fl.NumOfPage++
}

// Remove はページをフラッシュリストから削除する
func (fl *FlushList) Remove(pageId page.PageId) {
	node, exists := fl.nodeMap[pageId]
	if !exists {
		return
	}

	if node.prev != nil {
		node.prev.next = node.next
	} else {
		fl.head = node.next
	}

	if node.next != nil {
		node.next.prev = node.prev
	} else {
		fl.tail = node.prev
	}

	delete(fl.nodeMap, pageId)
	fl.NumOfPage--
}

// Clear はフラッシュリスト全体をクリアする
func (fl *FlushList) Clear() {
	fl.head = nil
	fl.tail = nil
	fl.NumOfPage = 0
	fl.nodeMap = make(map[page.PageId]*flushListNode)
}

// OldestPageIds は先頭 (最も古い) から n 件の PageId を返す
func (fl *FlushList) OldestPageIds(n int) []page.PageId {
	result := []page.PageId{}
	node := fl.head
	for node != nil && len(result) < n {
		result = append(result, node.pageId)
		node = node.next
	}
	return result
}
