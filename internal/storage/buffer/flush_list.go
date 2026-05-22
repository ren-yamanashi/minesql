package buffer

import (
	"github.com/ren-yamanashi/minesql/internal/storage/page"
)

type FlushListNode struct {
	PageId page.PageId // このノードが表すページの PageId
	Prev   *FlushListNode
	Next   *FlushListNode
}

// FlushList はダーティーページをダーティーになった順に管理する双方向リンクリスト
type FlushList struct {
	NumOfPage int            // リスト内のページ数
	Head      *FlushListNode // 最も古いダーティーページ
	Tail      *FlushListNode // 最も新しいダーティーページ
	nodeMap   map[page.PageId]*FlushListNode
}

func NewFlushList() *FlushList {
	return &FlushList{
		nodeMap: make(map[page.PageId]*FlushListNode),
	}
}

// Add はページをフラッシュリストの末尾に追加する
func (fl *FlushList) Add(pageId page.PageId) {
	if _, exists := fl.nodeMap[pageId]; exists {
		return
	}

	node := &FlushListNode{PageId: pageId}
	fl.nodeMap[pageId] = node

	if fl.Tail == nil {
		fl.Head = node
		fl.Tail = node
	} else {
		node.Prev = fl.Tail
		fl.Tail.Next = node
		fl.Tail = node
	}
	fl.NumOfPage++
}

// Delete はページをフラッシュリストから削除する
func (fl *FlushList) Delete(pageId page.PageId) {
	node, exists := fl.nodeMap[pageId]
	if !exists {
		return
	}

	if node.Prev != nil {
		node.Prev.Next = node.Next
	} else {
		fl.Head = node.Next
	}

	if node.Next != nil {
		node.Next.Prev = node.Prev
	} else {
		fl.Tail = node.Prev
	}

	delete(fl.nodeMap, pageId)
	fl.NumOfPage--
}

// Clear はフラッシュリスト全体をクリアする
func (fl *FlushList) Clear() {
	fl.Head = nil
	fl.Tail = nil
	fl.NumOfPage = 0
	fl.nodeMap = make(map[page.PageId]*FlushListNode)
}

// OldestPageIds は先頭 (最も古い) から n 件の PageId を返す
func (fl *FlushList) OldestPageIds(n int) []page.PageId {
	result := make([]page.PageId, 0, n)
	node := fl.Head
	for node != nil && len(result) < n {
		result = append(result, node.PageId)
		node = node.Next
	}
	return result
}
