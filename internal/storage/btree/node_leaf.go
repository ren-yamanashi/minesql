package btree

import (
	"bytes"

	"github.com/ren-yamanashi/minesql/internal/storage/buffer"
	"github.com/ren-yamanashi/minesql/internal/storage/page"
)

// リーフノードヘッダー内のオフセット
const (
	leafNodePrevPageIdOffset = 0
	leafNodeNextPageIdOffset = 8
	leafNodeHeaderSize       = 16
)

type leafNode struct {
	// ノードタイプヘッダー + リーフノードヘッダーの読み取りビュー (書き込みは bufPage の API 経由で行う必要がある)
	//   - header[0:8]: ノードタイプ
	//   - header[8:16]: prev PageId
	//   - header[16:24]: next PageId
	header  []byte
	body    *slottedPage
	bufPage *buffer.Page
}

func newLeafNode(bufPage *buffer.Page) *leafNode {
	data := bufPage.Data().Body()
	headerSize := nodeHeaderSize + leafNodeHeaderSize
	header := data[:headerSize]
	body := newSlottedPage(bufPage, headerSize)
	return &leafNode{
		header:  header,
		body:    body,
		bufPage: bufPage,
	}
}

// initialize はリーフノードを初期化する
//
// 初期化時には、ノードタイプヘッダーを設定し、前後のリーフノードのポインタ (PageId) には無効値が設定される
func (ln *leafNode) initialize() {
	ln.bufPage.WriteBodyAt(0, []byte(nodeTypeLeaf))
	invalidId := page.InvalidId().Bytes()
	ln.bufPage.WriteBodyAt(nodeHeaderSize+leafNodePrevPageIdOffset, invalidId)
	ln.bufPage.WriteBodyAt(nodeHeaderSize+leafNodeNextPageIdOffset, invalidId)
	ln.body.initialize()
}

// insert はレコードを挿入する
//   - slotNum: 挿入先のスロット番号
//   - record: 挿入するレコード
//   - return: 挿入に成功した場合は true
func (ln *leafNode) insert(slotNum int, record Record) bool {
	recordBytes := record.Bytes()
	if len(recordBytes) > ln.maxRecordSize() {
		return false
	}
	return ln.body.insert(slotNum, recordBytes)
}

// canFit は record を分割せずにこのリーフノードに挿入できるかを返す
func (ln *leafNode) canFit(record Record) bool {
	recordBytes := record.Bytes()
	if len(recordBytes) > ln.maxRecordSize() {
		return false
	}
	return ln.body.hasSpaceFor(len(recordBytes))
}

// splitInsert はリーフノードを分割しながらレコードを挿入する
//   - newLeaf: 分割後の新しいリーフノード (小さい方のレコードが格納される)
//   - newRecord: 挿入するレコード
//   - return: 古いノード (=右の子) の最小キー (=親ブランチノードの境界キー)
func (ln *leafNode) splitInsert(newLeaf *leafNode, newRecord Record) []byte {
	newLeaf.initialize()
	for {
		if newLeaf.isHalfFull() {
			slotNum, _ := ln.searchSlotNum(newRecord.Key())
			if !ln.insert(slotNum, newRecord) {
				panic("btree: old leaf node must have space after split")
			}
			break
		}

		// `古いノードの先頭レコードのキー < 挿入対象のキー` の場合
		if ln.record(0).compareKey(newRecord.Key()) < 0 {
			ln.transfer(newLeaf)
			continue
		}

		// `古いノードの先頭レコードのキー >= 挿入対象のキー` の場合
		if !newLeaf.insert(newLeaf.numRecords(), newRecord) {
			panic("btree: new leaf node must have space for split target")
		}
		for !newLeaf.isHalfFull() {
			ln.transfer(newLeaf)
		}
		break
	}
	return bytes.Clone(ln.record(0).Key())
}

// delete はレコードを削除する
func (ln *leafNode) delete(slotNum int) {
	ln.body.delete(slotNum)
}

// canDeleteWithoutUnderflow は指定スロットを削除してもノードが半分以上埋まったままかを返す
func (ln *leafNode) canDeleteWithoutUnderflow(slotNum int) bool {
	recordSize := len(ln.record(slotNum).Bytes())
	used := ln.body.capacity() - ln.body.freeSpace()
	afterDelete := used - slottedPagePointerSize - recordSize
	return afterDelete > ln.body.capacity()/2
}

// update は指定されたスロットのレコードを更新する
//   - slotNum: 更新するレコードのスロット番号
//   - record: 新しいレコード (key は変更されない前提)
func (ln *leafNode) update(slotNum int, record Record) bool {
	return ln.body.update(slotNum, record.Bytes())
}

// canFitUpdate は指定スロットの record を新しい record に更新できるかを返す
func (ln *leafNode) canFitUpdate(slotNum int, record Record) bool {
	recordBytes := record.Bytes()
	if len(recordBytes) > ln.maxRecordSize() {
		return false
	}
	return ln.body.canResize(slotNum, len(recordBytes))
}

// numRecords はレコード数を取得する
func (ln *leafNode) numRecords() int {
	return ln.body.numSlots()
}

// canTransferRecord は兄弟ノードにレコードを転送できるか判定する
//   - isLeftSibling: true の場合は左の兄弟からの転送 (末尾レコードが転送対象)
//   - return: 転送後も半分以上埋まっている場合は true を返す
func (ln *leafNode) canTransferRecord(isLeftSibling bool) bool {
	if ln.numRecords() <= 1 {
		return false
	}

	// 左の兄弟の場合は末尾レコード、右の兄弟の場合は先頭レコードが転送対象
	var targetIndex int
	if isLeftSibling {
		targetIndex = ln.numRecords() - 1
	}
	targetRecordData := ln.body.cell(targetIndex)
	targetRecordSize := len(targetRecordData)

	freeSpaceAfterTransfer := ln.body.freeSpace() + targetRecordSize + slottedPagePointerSize
	return 2*freeSpaceAfterTransfer < ln.body.capacity()
}

// record は指定されたスロット番号のレコードを取得する
func (ln *leafNode) record(slotNum int) Record {
	return recordFromBytes(ln.body.cell(slotNum))
}

// searchSlotNum は指定された key に対応するスロット番号を検索する
//   - 見つかった場合: (スロット番号, true)
//   - 見つからなかった場合: (挿入すべき位置, false)
func (ln *leafNode) searchSlotNum(key []byte) (int, bool) {
	return binarySearch(ln, key)
}

// prevPageId は前のリーフノードのページ ID を取得する
func (ln *leafNode) prevPageId() page.Id {
	return page.ReadId(ln.header[nodeHeaderSize:], leafNodePrevPageIdOffset)
}

// nextPageId は次のリーフノードのページ ID を取得する
func (ln *leafNode) nextPageId() page.Id {
	return page.ReadId(ln.header[nodeHeaderSize:], leafNodeNextPageIdOffset)
}

// setPrevPageId は前のリーフノードのページ ID を設定する
func (ln *leafNode) setPrevPageId(prevPageId page.Id) {
	ln.bufPage.WriteBodyAt(nodeHeaderSize+leafNodePrevPageIdOffset, prevPageId.Bytes())
}

// setNextPageId は次のリーフノードのページ ID を設定する
func (ln *leafNode) setNextPageId(nextPageId page.Id) {
	ln.bufPage.WriteBodyAt(nodeHeaderSize+leafNodeNextPageIdOffset, nextPageId.Bytes())
}

// transferAllFrom は src のすべてのレコードを自分の末尾に転送する (src のレコードはすべて削除される)
func (ln *leafNode) transferAllFrom(src *leafNode) bool {
	return src.body.transferAllTo(ln.body)
}

// canMergeAllFrom は src の全レコードを自分の末尾に取り込めるかを、書き込まずに返す
func (ln *leafNode) canMergeAllFrom(src *leafNode) bool {
	return src.body.canTransferAllTo(ln.body)
}

// isHalfFull はリーフノードが半分以上埋まっているかどうかを判定する
func (ln *leafNode) isHalfFull() bool {
	return 2*ln.body.freeSpace() < ln.body.capacity()
}

// maxRecordSize は自身のノードに格納できる最大のレコードサイズを返す
func (ln *leafNode) maxRecordSize() int {
	// /2: ノード分割時に各ノードが半分以上埋まることを保証するため、1 レコードは容量の半分以下でなければならない
	// -pointerSize: Slotted Page ではレコードごとに pointer が必要なため、その分を差し引く
	return ln.body.capacity()/2 - slottedPagePointerSize
}

// transfer は先頭のレコードを別のリーフノードに移動する
func (ln *leafNode) transfer(dest *leafNode) {
	nextIndex := dest.numRecords()
	data := ln.body.cell(0)

	if !dest.body.insert(nextIndex, data) {
		panic("btree: dest leaf node must have space for transfer")
	}

	ln.body.delete(0)
}
