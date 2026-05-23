package buffer

import (
	"github.com/ren-yamanashi/minesql/internal/storage/page"
)

type flushListNode struct {
	pageId page.Id // このノードが表すページの PageId
	prev   *flushListNode
	next   *flushListNode
}

// flushList はダーティーページをダーティーになった順に管理する双方向リンクリスト
type flushList struct {
	pageCount int            // リスト内のページ数
	head      *flushListNode // 最も古いダーティーページ
	tail      *flushListNode // 最も新しいダーティーページ
	nodeMap   map[page.Id]*flushListNode
}

func newFlushList() *flushList {
	return &flushList{
		nodeMap: make(map[page.Id]*flushListNode),
	}
}

// add はページをフラッシュリストの末尾に追加する
func (fl *flushList) add(pageId page.Id) {
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
	fl.pageCount++
}

// delete はページをフラッシュリストから削除する
func (fl *flushList) delete(pageId page.Id) {
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
	fl.pageCount--
}

// clear はフラッシュリスト全体をクリアする
func (fl *flushList) clear() {
	fl.head = nil
	fl.tail = nil
	fl.pageCount = 0
	fl.nodeMap = make(map[page.Id]*flushListNode)
}

// oldestPageIds は先頭 (最も古い) から n 件の PageId を返す
func (fl *flushList) oldestPageIds(n int) []page.Id {
	result := make([]page.Id, 0, n)
	node := fl.head
	for node != nil && len(result) < n {
		result = append(result, node.pageId)
		node = node.next
	}
	return result
}

// forEach は全ノードに対してコールバックを実行する
func (fl *flushList) forEach(fn func(pageId page.Id)) {
	for node := fl.head; node != nil; node = node.next {
		fn(node.pageId)
	}
}
