package node

import (
	"errors"

	"github.com/ren-yamanashi/minesql/internal/storage/page"
)

const leafHeaderSize = 16

// リーフノードヘッダー内のオフセット
const (
	leafPrevPageIdOffset = 0
	leafNextPageIdOffset = 8
)

type LeafNode struct {
	// ノードタイプヘッダー + リーフノードヘッダー
	//   - header[0:8]: ノードタイプ
	//   - header[8:16]: prev PageId
	//   - header[16:24]: next PageId
	header []byte
	nodeBody
}

func NewLeafNode(pg *page.Page) *LeafNode {
	data := pg.Body
	copy(data[0:8], NodeTypeLeaf)
	headerSize := nodeHeaderSize + leafHeaderSize
	header := data[:headerSize]
	body := NewSlottedPage(data[headerSize:])
	return &LeafNode{
		header:   header,
		nodeBody: nodeBody{body: body},
	}
}

// Initialize はリーフノードを初期化する
//
// 初期化時には、前後のリーフノードのポインタ (PageId) には無効値が設定される
func (ln *LeafNode) Initialize() {
	page.InvalidPageId.WriteTo(ln.header[nodeHeaderSize:], leafPrevPageIdOffset)
	page.InvalidPageId.WriteTo(ln.header[nodeHeaderSize:], leafNextPageIdOffset)
	ln.body.Initialize()
}

// SplitInsert はリーフノードを分割しながらレコードを挿入する
//   - newLeaf: 分割後の新しいリーフノード (小さい方のレコードが格納される)
//   - newRecord: 挿入するレコード
//   - return: 古いノード (=右の子) の最小キー (=親ブランチノードの境界キー)
func (ln *LeafNode) SplitInsert(newLeaf *LeafNode, newRecord Record) ([]byte, error) {
	newLeaf.Initialize()
	for {
		if newLeaf.IsHalfFull() {
			slotNum, _ := ln.SearchSlotNum(newRecord.Key())
			if !ln.Insert(slotNum, newRecord) {
				return nil, errors.New("old leaf node must have space")
			}
			break
		}

		// `古いノードの先頭レコードのキー < 挿入対象のキー` の場合
		if ln.Record(0).CompareKey(newRecord.Key()) < 0 {
			if err := ln.transfer(&newLeaf.nodeBody); err != nil {
				return nil, err
			}
			continue
		}

		// `古いノードの先頭レコードのキー >= 挿入対象のキー` の場合
		if !newLeaf.Insert(newLeaf.NumRecords(), newRecord) {
			return nil, errors.New("new leaf node must have space")
		}
		for !newLeaf.IsHalfFull() {
			if err := ln.transfer(&newLeaf.nodeBody); err != nil {
				return nil, err
			}
		}
		break
	}
	return ln.Record(0).Key(), nil
}

// PrevPageId は前のリーフノードのページ ID を取得する
func (ln *LeafNode) PrevPageId() page.PageId {
	return page.ReadPageId(ln.header[nodeHeaderSize:], leafPrevPageIdOffset)
}

// NextPageId は次のリーフノードのページ ID を取得する
func (ln *LeafNode) NextPageId() page.PageId {
	return page.ReadPageId(ln.header[nodeHeaderSize:], leafNextPageIdOffset)
}

// SetPrevPageId は前のリーフノードのページ ID を設定する
func (ln *LeafNode) SetPrevPageId(prevPageId page.PageId) {
	prevPageId.WriteTo(ln.header[nodeHeaderSize:], leafPrevPageIdOffset)
}

// SetNextPageId は次のリーフノードのページ ID を設定する
func (ln *LeafNode) SetNextPageId(nextPageId page.PageId) {
	nextPageId.WriteTo(ln.header[nodeHeaderSize:], leafNextPageIdOffset)
}

// TransferAllFrom は src のすべてのレコードを自分の末尾に転送する (src のレコードはすべて削除される)
func (ln *LeafNode) TransferAllFrom(src *LeafNode) bool {
	return src.body.TransferAllTo(ln.body)
}
