package buffer

import (
	"encoding/binary"
	"minesql/internal/storage/page"
)

type flushListNode struct {
	pageId page.PageId    // このノードが表すページの ID
	prev   *flushListNode // 前のノード
	next   *flushListNode // 次のノード
}

// FlushList はダーティーページをダーティーになった順に管理する双方向リンクリスト
//
// 先頭 (head) が最も古く、末尾 (tail) が最も新しい。ページクリーナーは先頭から順にフラッシュする。
type FlushList struct {
	Size    int                            // リスト内のページ数
	head    *flushListNode                 // 最も古いダーティーページ (先にフラッシュ)
	tail    *flushListNode                 // 最も新しいダーティーページ
	nodeMap map[page.PageId]*flushListNode // PageId からノードへの逆引きマップ (O(1) の検索・削除用)
}

// NewFlushList は空のフラッシュリストを生成する
func NewFlushList() *FlushList {
	return &FlushList{
		nodeMap: make(map[page.PageId]*flushListNode),
	}
}

// Add はページをフラッシュリストの末尾に追加する
func (fl *FlushList) Add(pageId page.PageId) {
	// 既にリストに含まれている場合は何もしない (最初にダーティーになった順序を維持するため)
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
	fl.Size++
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
	fl.Size--
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

// Clear はフラッシュリスト全体をクリアする
func (fl *FlushList) Clear() {
	fl.head = nil
	fl.tail = nil
	fl.Size = 0
	fl.nodeMap = make(map[page.PageId]*flushListNode)
}

// MinPageLSN はフラッシュリスト内の全ダーティーページの最小 Page LSN を返す
//
// ダーティーページがない場合は 0 を返す
func (fl *FlushList) MinPageLSN(bufferPages []BufferPage, pageTable PageTable) uint32 {
	if fl.Size == 0 {
		return 0
	}

	var minLSN uint32
	first := true
	for node := fl.head; node != nil; node = node.next {
		bufferId, ok := pageTable[node.pageId]
		if !ok {
			continue
		}
		pg := page.NewPage(bufferPages[bufferId].Page)
		lsn := binary.BigEndian.Uint32(pg.Header)
		if first || lsn < minLSN {
			minLSN = lsn
			first = false
		}
	}
	return minLSN
}

// Contains は指定ページがフラッシュリストに含まれているかを返す
func (fl *FlushList) Contains(pageId page.PageId) bool {
	_, exists := fl.nodeMap[pageId]
	return exists
}
