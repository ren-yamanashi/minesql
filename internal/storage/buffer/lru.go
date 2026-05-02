package buffer

type lruNode struct {
	bufferId BufferId
	prev     *lruNode
	next     *lruNode
	isOld    bool // OldSublist に所属しているか
	isUnused bool // 未使用 or 追い出し直後
}

type LRU struct {
	head     *lruNode              // リストの先頭 (NewSublist の先頭)
	tail     *lruNode              // リストの末尾 (OldSublist の末尾 = 追い出し候補)
	midpoint *lruNode              // OldSublist の先頭 (midpoint)
	nodeMap  map[BufferId]*lruNode // BufferId → ノードの参照
	newLen   int                   // NewSublist の現在の長さ
	oldLen   int                   // OldSublist の現在の長さ
	maxNew   int                   // NewSublist の最大長 (全体の 5/8)
}

func NewLRU(numOfPage int) *LRU {
	lru := &LRU{
		nodeMap: make(map[BufferId]*lruNode, numOfPage),
		maxNew:  numOfPage * 5 / 8,
	}
	// 初期状態では全て未使用なので、全スロットを OldSublist に追加
	for i := range numOfPage {
		node := &lruNode{
			bufferId: BufferId(i),
			isOld:    true,
			isUnused: true,
		}
		lru.nodeMap[BufferId(i)] = node
		lru.insertToTail(node)
	}
	lru.midpoint = lru.head // 初期時点では全て OldSublist に属しているため、midpoint はリストの先頭を指す
	lru.oldLen = numOfPage
	return lru
}

// Access はページがアクセスされたことを記録する
func (l *LRU) Access(bufferId BufferId) {
	node := l.nodeMap[bufferId]

	// 新規ページにアクセスした場合: midpoint に配置
	if node.isUnused {
		node.isUnused = false
		l.moveToMidpoint(node)
		return
	}

	// OldSublist 内のページに再アクセスした場合: NewSublist の先頭に昇格
	if node.isOld {
		l.promoteToNew(node)
		return
	}

	// NewSublist 内のページに再アクセスした場合: NewSublist の先頭に移動
	l.moveToNewHead(node)
}

// Evict は追い出すページの BufferId を返す
func (l *LRU) Evict() BufferId {
	victim := l.tail
	victim.isUnused = true
	return victim.bufferId
}

// Delete はページの参照を解除し、優先的に追い出されるようにする
func (l *LRU) Delete(bufferId BufferId) {
	node := l.nodeMap[bufferId]
	l.moveToOldTail(node)
}

// moveToMidpoint はノードを midpoint (OldSublist の先頭) に配置する
func (l *LRU) moveToMidpoint(node *lruNode) {
	if node == l.midpoint {
		return
	}
	l.detach(node)

	if l.midpoint != nil {
		l.insertBefore(l.midpoint, node)
	} else {
		l.insertToTail(node)
	}

	node.isOld = true
	l.oldLen++
	l.midpoint = node
}

// moveToNewHead は NewSublist 内のノードを先頭に移動する
func (l *LRU) moveToNewHead(node *lruNode) {
	if l.head == node {
		return
	}
	l.detach(node)
	l.prependToHead(node)
	l.newLen++
}

// moveToOldTail はノードを OldSublist の末尾に移動する
func (l *LRU) moveToOldTail(node *lruNode) {
	if l.tail == node {
		node.isOld = true
		return
	}
	l.detach(node)
	l.insertToTail(node)
	node.isOld = true
	l.oldLen++
}

// rebalance は NewSublist が最大長を超えた場合、midpoint を前方に移動して NewSublist の末尾ノードを OldSublist に降格する
func (l *LRU) rebalance() {
	for l.newLen > l.maxNew {
		switch {
		case l.midpoint == nil:
			// OldSublist が空の場合、リスト末尾を OldSublist に降格
			l.midpoint = l.tail
		case l.midpoint.prev == nil:
			return
		default:
			l.midpoint = l.midpoint.prev
		}
		l.midpoint.isOld = true
		l.newLen--
		l.oldLen++
	}
}

// detach はノードをリストから切り離す
func (l *LRU) detach(node *lruNode) {
	if node == l.midpoint {
		l.midpoint = node.next
	}
	if node.prev != nil {
		node.prev.next = node.next
	} else {
		l.head = node.next
	}
	if node.next != nil {
		node.next.prev = node.prev
	} else {
		l.tail = node.prev
	}
	node.prev = nil
	node.next = nil
	if node.isOld {
		l.oldLen--
	} else {
		l.newLen--
	}
}

// insertBefore は target の直前にノードを挿入する
func (l *LRU) insertBefore(target, node *lruNode) {
	node.next = target
	node.prev = target.prev
	if target.prev != nil {
		target.prev.next = node
	} else {
		l.head = node
	}
	target.prev = node
}

// insertToTail はリストの末尾にノードを追加する
func (l *LRU) insertToTail(node *lruNode) {
	node.next = nil
	node.prev = l.tail
	if l.tail != nil {
		l.tail.next = node
	} else {
		l.head = node
	}
	l.tail = node
}

// promoteToNew は OldSublist のノードを NewSublist の先頭に昇格する
func (l *LRU) promoteToNew(node *lruNode) {
	l.detach(node)
	l.prependToHead(node)
	node.isOld = false
	l.newLen++
	l.rebalance()
}

// prependToHead はリストの先頭にノードを追加する
func (l *LRU) prependToHead(node *lruNode) {
	node.prev = nil
	node.next = l.head
	if l.head != nil {
		l.head.prev = node
	} else {
		l.tail = node
	}
	l.head = node
}
