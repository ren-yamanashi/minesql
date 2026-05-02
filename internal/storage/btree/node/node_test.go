package node

import (
	"testing"

	"github.com/ren-yamanashi/minesql/internal/storage/page"
	"github.com/stretchr/testify/assert"
)

func TestGetNodeType(t *testing.T) {
	t.Run("リーフノードのタイプを取得できる", func(t *testing.T) {
		// GIVEN
		data := make([]byte, page.PageSize)
		copy(data[page.PageHeaderSize:page.PageHeaderSize+8], NodeTypeLeaf)
		pg, _ := page.NewPage(data)

		// WHEN
		result := GetNodeType(pg)

		// THEN
		assert.Equal(t, NodeTypeLeaf, result)
	})

	t.Run("ブランチノードのタイプを取得できる", func(t *testing.T) {
		// GIVEN
		data := make([]byte, page.PageSize)
		copy(data[page.PageHeaderSize:page.PageHeaderSize+8], NodeTypeBranch)
		pg, _ := page.NewPage(data)

		// WHEN
		result := GetNodeType(pg)

		// THEN
		assert.Equal(t, NodeTypeBranch, result)
	})
}
