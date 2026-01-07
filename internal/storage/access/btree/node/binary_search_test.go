package node

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestBinarySearch(t *testing.T) {
	t.Run("正常に見つかる場合、要素のインデックスを返す", func(t *testing.T) {
		arr := []int{1, 3, 5, 7, 9}
		target := 5

		index, found := binarySearch(len(arr), func(i int) int {
			return compare(arr[i], target)
		})

		assert.True(t, found)
		assert.Equal(t, 2, index) // value 5 のインデックスは 2
	})

	t.Run("見つからない場合、挿入すべき位置のインデックスを返す", func(t *testing.T) {
		arr := []int{1, 3, 5, 7, 9}
		target := 6

		index, found := binarySearch(len(arr), func(i int) int {
			return compare(arr[i], target)
		})

		assert.False(t, found)
		assert.Equal(t, 3, index) // 挿入すべき位置はインデックス 3 (5 と 7 の間)
	})

	t.Run("空の配列の場合", func(t *testing.T) {
		arr := []int{}
		target := 1

		index, found := binarySearch(len(arr), func(i int) int {
			return compare(arr[i], target)
		})

		assert.False(t, found)
		assert.Equal(t, 0, index) // 挿入すべき位置はインデックス 0
	})
}

func compare(a, b int) int {
	if a < b {
		return -1
	} else if a > b {
		return 1
	} else {
		return 0
	}
}
