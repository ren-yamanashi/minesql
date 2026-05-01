package node

// 二分探索を行う
//   - node: 探索対象のノード
//   - key: 探索するキー
//   - 見つかった場合: (要素のインデックス, true)
//   - 見つからなかった場合: (挿入すべき位置のインデックス, false)
func binarySearch(node Node, key []byte) (int, bool) {
	var left int
	right := node.NumRecords()

	for left < right {
		mid := left + (right-left)/2
		record := node.RecordAt(mid) // "1ノード=1ページ" であるため、`mid=slotNum` として該当のレコードを取得可能

		switch record.CompareKey(key) {
		case 0:
			return mid, true
		case -1: // record.Key < key の場合、右側に進む
			left = mid + 1
		case 1: // record.Key > key の場合、左側に進む
			right = mid
		}
	}

	return left, false
}
