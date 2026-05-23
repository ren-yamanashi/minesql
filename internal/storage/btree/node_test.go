package btree

import (
	"testing"

	"github.com/ren-yamanashi/minesql/internal/storage/page"
	"github.com/stretchr/testify/assert"
)

func TestNodeType(t *testing.T) {
	t.Run("リーフノードのタイプを取得できる", func(t *testing.T) {
		// GIVEN
		data := make([]byte, page.PageSize)
		copy(data[page.PageHeaderSize:page.PageHeaderSize+8], nodeTypeLeaf)
		pg, _ := page.NewPage(data)

		// WHEN
		result := nodeType(pg)

		// THEN
		assert.Equal(t, nodeTypeLeaf, result)
	})

	t.Run("ブランチノードのタイプを取得できる", func(t *testing.T) {
		// GIVEN
		data := make([]byte, page.PageSize)
		copy(data[page.PageHeaderSize:page.PageHeaderSize+8], nodeTypeBranch)
		pg, _ := page.NewPage(data)

		// WHEN
		result := nodeType(pg)

		// THEN
		assert.Equal(t, nodeTypeBranch, result)
	})
}
