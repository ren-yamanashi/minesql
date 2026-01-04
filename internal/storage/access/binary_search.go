package access

// 二分探索を行う
// size: 探索対象の要素数
// f: 比較関数 (インデックスに対して、要素が探索対象の値より小さければ -1, 等しければ 0, 大きければ 1 を返す)
// 戻り値:
// - 見つかった場合: (要素のインデックス, true)
// - 見つからなかった場合: (挿入すべき位置のインデックス, false)
func binarySearch(size int, f func(int) int) (int, bool) {
	left := 0
	right := size

	for left < right {
		mid := left + (right-left)/2
		result := f(mid)

		switch {
		case result == 0:
			return mid, true
		case result == -1:
			left = mid + 1
		case result == 1:
			right = mid
		}
	}

	return left, false
}
