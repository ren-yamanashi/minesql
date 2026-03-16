package node

// 二分探索を行う
// node: 探索対象のノード
// key: 探索するキー
// 戻り値:
// - 見つかった場合: (要素のインデックス, true)
// - 見つからなかった場合: (挿入すべき位置のインデックス, false)
func binarySearch(node Node, key []byte) (int, bool) {
	left := 0
	right := node.NumPairs()

	for left < right {
		mid := left + (right-left)/2
		pair := node.PairAt(mid) // "1ノード=1ページ" であるため、`mid=slotNum` として該当の key-value ペアを取得できる

		// pair のキーと探索するキーを比較
		// -1: pair.Key < key
		// 0: pair.Key == key
		// 1: pair.Key > key
		result := pair.CompareKey(key)

		switch result {
		// キーが見つかった場合、要素のインデックスを返す
		case 0:
			return mid, true
		// キーが見つからない場合、左右どちらに進むべきかを決定する
		// pair.Key < key の場合、右側に進むため left を mid + 1 に更新する (mid の右半分に対して同様の流れで探索を続ける)
		case -1:
			left = mid + 1
		// pair.Key > key の場合、左側に進むため right を mid に更新する (mid の左半分に対して同様の流れで探索を続ける)
		case 1:
			right = mid
		}
	}

	return left, false
}
