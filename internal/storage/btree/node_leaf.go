package btree

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

type leafNode struct {
	// ノードタイプヘッダー + リーフノードヘッダー
	//   - header[0:8]: ノードタイプ
	//   - header[8:16]: prev PageId
	//   - header[16:24]: next PageId
	header []byte
	body   *slottedPage
}

func newLeafNode(pg *page.Page) *leafNode {
	data := pg.Body
	copy(data[0:8], nodeTypeLeaf)
	headerSize := nodeHeaderSize + leafHeaderSize
	header := data[:headerSize]
	body := newSlottedPage(data[headerSize:])
	return &leafNode{
		header: header,
		body:   body,
	}
}

// initialize はリーフノードを初期化する
//
// 初期化時には、前後のリーフノードのポインタ (PageId) には無効値が設定される
func (ln *leafNode) initialize() {
	page.InvalidId.WriteTo(ln.header[nodeHeaderSize:], leafPrevPageIdOffset)
	page.InvalidId.WriteTo(ln.header[nodeHeaderSize:], leafNextPageIdOffset)
	ln.body.initialize()
}

// insert はレコードを挿入する
//   - slotNum: 挿入先のスロット番号
//   - record: 挿入するレコード
//   - return: 挿入に成功した場合は true
func (ln *leafNode) insert(slotNum int, record Record) bool {
	recordBytes := record.ToBytes()
	if len(recordBytes) > ln.maxRecordSize() {
		return false
	}
	return ln.body.insert(slotNum, recordBytes)
}

// splitInsert はリーフノードを分割しながらレコードを挿入する
//   - newLeaf: 分割後の新しいリーフノード (小さい方のレコードが格納される)
//   - newRecord: 挿入するレコード
//   - return: 古いノード (=右の子) の最小キー (=親ブランチノードの境界キー)
func (ln *leafNode) splitInsert(newLeaf *leafNode, newRecord Record) ([]byte, error) {
	newLeaf.initialize()
	for {
		if newLeaf.isHalfFull() {
			slotNum, _ := ln.searchSlotNum(newRecord.Key())
			if !ln.insert(slotNum, newRecord) {
				return nil, errors.New("old leaf node must have space")
			}
			break
		}

		// `古いノードの先頭レコードのキー < 挿入対象のキー` の場合
		if ln.record(0).CompareKey(newRecord.Key()) < 0 {
			if err := ln.transfer(newLeaf); err != nil {
				return nil, err
			}
			continue
		}

		// `古いノードの先頭レコードのキー >= 挿入対象のキー` の場合
		if !newLeaf.insert(newLeaf.numRecords(), newRecord) {
			return nil, errors.New("new leaf node must have space")
		}
		for !newLeaf.isHalfFull() {
			if err := ln.transfer(newLeaf); err != nil {
				return nil, err
			}
		}
		break
	}
	return ln.record(0).Key(), nil
}

// delete はレコードを削除する
func (ln *leafNode) delete(slotNum int) {
	ln.body.delete(slotNum)
}

// update は指定されたスロットのレコードを更新する
//   - slotNum: 更新するレコードのスロット番号
//   - record: 新しいレコード (key は変更されない前提)
func (ln *leafNode) update(slotNum int, record Record) bool {
	return ln.body.update(slotNum, record.ToBytes())
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
//   - 見つからなかった場合: (0, false)
func (ln *leafNode) searchSlotNum(key []byte) (int, bool) {
	return binarySearch(ln, key)
}

// prevPageId は前のリーフノードのページ ID を取得する
func (ln *leafNode) prevPageId() page.Id {
	return page.ReadId(ln.header[nodeHeaderSize:], leafPrevPageIdOffset)
}

// nextPageId は次のリーフノードのページ ID を取得する
func (ln *leafNode) nextPageId() page.Id {
	return page.ReadId(ln.header[nodeHeaderSize:], leafNextPageIdOffset)
}

// setPrevPageId は前のリーフノードのページ ID を設定する
func (ln *leafNode) setPrevPageId(prevPageId page.Id) {
	prevPageId.WriteTo(ln.header[nodeHeaderSize:], leafPrevPageIdOffset)
}

// setNextPageId は次のリーフノードのページ ID を設定する
func (ln *leafNode) setNextPageId(nextPageId page.Id) {
	nextPageId.WriteTo(ln.header[nodeHeaderSize:], leafNextPageIdOffset)
}

// transferAllFrom は src のすべてのレコードを自分の末尾に転送する (src のレコードはすべて削除される)
func (ln *leafNode) transferAllFrom(src *leafNode) bool {
	return src.body.transferAllTo(ln.body)
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
func (ln *leafNode) transfer(dest *leafNode) error {
	nextIndex := dest.numRecords()
	data := ln.body.cell(0)

	if !dest.body.insert(nextIndex, data) {
		return errors.New("no space in dest leaf node")
	}

	ln.body.delete(0)
	return nil
}
