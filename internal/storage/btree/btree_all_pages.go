package btree

import (
	"github.com/ren-yamanashi/minesql/internal/storage/buffer"
	"github.com/ren-yamanashi/minesql/internal/storage/page"
)

// AllPageIds は B+Tree 配下の全ページ (メタページ + 内部ノード + リーフ) の PageId を BFS 順に返す
//   - mtr: 各ページを X ラッチで保持する書き込み用 Mtr (呼び出し側で生成・解放する)
//   - 戻り順: 親階層 → 子階層 (= メタページ先頭、リーフ末尾)。 解放時は逆順に解放する想定
func (t *Tree) AllPageIds(mtr *buffer.Mtr) ([]page.Id, error) {
	metaBufPage, err := mtr.PageForWrite(t.MetaPageId())
	if err != nil {
		return nil, err
	}
	metaPage := newMetaPage(metaBufPage)
	rootPageId := metaPage.rootPageId()
	height := metaPage.height()
	mtr.Unpin(t.MetaPageId())

	result := []page.Id{t.MetaPageId()}

	currentLevel := []page.Id{rootPageId}
	for range max(height, 1) - 1 {
		var nextLevel []page.Id
		for _, nodePageId := range currentLevel {
			pg, err := mtr.PageForWrite(nodePageId)
			if err != nil {
				return nil, err
			}
			branchNode := newBranchNode(pg)
			for idx := range branchNode.numRecords() {
				childPageId, err := branchNode.childPageId(idx)
				if err != nil {
					mtr.Unpin(nodePageId)
					return nil, err
				}
				nextLevel = append(nextLevel, childPageId)
			}
			nextLevel = append(nextLevel, branchNode.rightChildPageId())
			result = append(result, nodePageId)
			mtr.Unpin(nodePageId)
		}
		currentLevel = nextLevel
	}

	for _, leafPageId := range currentLevel {
		if _, err := mtr.PageForWrite(leafPageId); err != nil {
			return nil, err
		}
		result = append(result, leafPageId)
		mtr.Unpin(leafPageId)
	}
	return result, nil
}
