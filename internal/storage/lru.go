package storage

type LRU struct {
	head     *lruNode              // リストの先頭 (NewSublist の先頭)
	tail     *lruNode              // リストの末尾 (OldSublist の末尾 = 追い出し候補)
	midpoint *lruNode              // OldSublist の先頭 (midpoint)
	nodeMap  map[BufferId]*lruNode // BufferId → ノードの参照
	newLen   int                   // NewSublist の現在の長さ
	oldLen   int                   // OldSublist の現在の長さ
	maxNew   int                   // NewSublist の最大長 (全体の 5/8)
}

type lruNode struct {
	bufferId BufferId // バッファ ID
	prev     *lruNode // リスト内で自分の前に位置するノードのポインタ
	next     *lruNode // リスト内で自分の後に位置するノードのポインタ
	isOld    bool     // OldSublist に属しているか
	isUnused bool     // 未使用スロット or 追い出し直後 (最初の Access で midpoint に配置)
}

// NewLRU は size をバッファプールの最大サイズとして LRU を初期化する
func NewLRU(size int) *LRU {
	lru := &LRU{
		nodeMap: make(map[BufferId]*lruNode, size),
		maxNew:  size * 5 / 8,
	}
	// 全スロットを OldSublist に追加 (初期状態では全て未使用)
	for i := range size {
		node := &lruNode{
			bufferId: BufferId(i),
			isOld:    true,
			isUnused: true,
		}
		lru.nodeMap[BufferId(i)] = node
		lru.appendToTail(node)
	}
	lru.midpoint = lru.head // 初期化時点では全て OldSublist に属しているため、midpoint はリストの先頭を指す
	lru.oldLen = size
	return lru
}

// Access はページがアクセスされたことを記録する
func (l *LRU) Access(bufferId BufferId) {
	node := l.nodeMap[bufferId]
	if node.isUnused {
		// 新規ページにアクセスした場合: midpoint (OldSublist の先頭) に配置
		node.isUnused = false
		l.moveToMidpoint(node)
		return
	}
	if node.isOld {
		// OldSublist 内のページに再アクセスした場合: NewSublist の先頭に昇格
		l.promoteToNew(node)
		return
	}
	// NewSublist 内のページに再アクセスした場合: NewSublist の先頭に移動
	l.moveToNewHead(node)
}

// Evict は追い出すページの BufferId を返す
func (l *LRU) Evict() BufferId {
	// 末尾 (OldSublist の末尾) を追い出し対象とする
	victim := l.tail
	victim.isUnused = true
	return victim.bufferId
}

// Remove はページの参照を解除し、優先的に追い出されるようにする
func (l *LRU) Remove(bufferId BufferId) {
	// OldSublist の末尾 (リスト末尾) に移動して、次の追い出しで優先的に選ばれるようにする
	node := l.nodeMap[bufferId]
	l.moveToOldTail(node)
}

// moveToMidpoint はノードを midpoint (OldSublist の先頭) に配置する
func (l *LRU) moveToMidpoint(node *lruNode) {
	if node == l.midpoint {
		return
	}
	l.detachAndUpdateCount(node)

	if l.midpoint != nil {
		l.insertBefore(l.midpoint, node)
	} else {
		// OldSublist が空の場合、リスト末尾に追加
		l.appendToTail(node)
	}
	node.isOld = true
	l.oldLen++
	l.midpoint = node
}

// promoteToNew は OldSublist のノードを NewSublist の先頭に昇格する
func (l *LRU) promoteToNew(node *lruNode) {
	l.detachAndUpdateCount(node)

	l.prependToHead(node)
	node.isOld = false
	l.newLen++

	l.rebalance()
}

// moveToNewHead は NewSublist 内のノードを先頭に移動する
func (l *LRU) moveToNewHead(node *lruNode) {
	if l.head == node {
		return
	}
	l.detach(node)
	l.prependToHead(node)
}

// moveToOldTail はノードを OldSublist の末尾 (リスト末尾) に移動する
func (l *LRU) moveToOldTail(node *lruNode) {
	if l.tail == node {
		node.isOld = true
		return
	}
	l.detachAndUpdateCount(node)

	l.appendToTail(node)
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

// detachAndUpdateCount はノードをリストから切り離し、所属するサブリストのカウントを更新する
func (l *LRU) detachAndUpdateCount(node *lruNode) {
	l.detach(node)
	if node.isOld {
		l.oldLen--
	} else {
		l.newLen--
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

// prependToHead　はリストの先頭にノードを追加する
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

// appendToTail はリストの末尾にノードを追加する
func (l *LRU) appendToTail(node *lruNode) {
	node.next = nil
	node.prev = l.tail
	if l.tail != nil {
		l.tail.next = node
	} else {
		l.head = node
	}
	l.tail = node
}
