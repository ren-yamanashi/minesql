package node

import (
	"errors"

	"github.com/ren-yamanashi/minesql/internal/storage/page"
)

const leafHeaderSize = 16

type LeafNode struct {
	// ノードタイプヘッダー + リーフノードヘッダー
	//   - header[0:8]: ノードタイプ
	//   - header[8:16]: prev PageId
	//   - header[16:24]: next PageId
	header []byte
	body   *SlottedPage
}

func NewLeafNode(data []byte) *LeafNode {
	copy(data[0:8], NodeTypeLeaf)
	headerSize := nodeHeaderSize + leafHeaderSize
	header := data[:headerSize]
	body := NewSlottedPage(data[headerSize:])
	return &LeafNode{
		header: header,
		body:   body,
	}
}

// Initialize はリーフノードを初期化する
//
// 初期化時には、前後のリーフノードのポインタ (PageId) には無効値が設定される
func (ln *LeafNode) Initialize() {
	page.InvalidPageId.WriteTo(ln.header[nodeHeaderSize:], 0)
	page.InvalidPageId.WriteTo(ln.header[nodeHeaderSize:], 8)
	ln.body.Initialize()
}

// Insert はレコードを挿入する
//   - slotNum: 挿入先のスロット番号
//   - record: 挿入するレコード
//   - return: 挿入に成功した場合は true
func (ln *LeafNode) Insert(slotNum int, record Record) bool {
	recordBytes := record.ToBytes()
	if len(recordBytes) > ln.maxRecordSize() {
		return false
	}
	return ln.body.Insert(slotNum, recordBytes)
}

// SplitInsert はリーフノードを分割しながらレコードを挿入する
//   - newLeaf: 分割後の新しいリーフノード
//   - newRecord: 挿入するレコード
//   - return: 新しいリーフノードの最小キー
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
			if err := ln.transfer(newLeaf); err != nil {
				return nil, err
			}
			continue
		}

		// `古いノードの先頭レコードのキー >= 挿入対象のキー` の場合
		if !newLeaf.Insert(newLeaf.NumRecords(), newRecord) {
			return nil, errors.New("new leaf node must have space")
		}
		for !newLeaf.IsHalfFull() {
			if err := ln.transfer(newLeaf); err != nil {
				return nil, err
			}
		}
		break
	}
	return newLeaf.Record(0).Key(), nil
}

// Remove はレコードを削除する
func (ln *LeafNode) Remove(slotNum int) {
	ln.body.Remove(slotNum)
}

// Update は指定されたスロットのレコードを更新する
//   - slotNum: 更新するレコードのスロット番号
//   - record: 新しいレコード (key は変更されない前提)
func (ln *LeafNode) Update(slotNum int, record Record) bool {
	return ln.body.Update(slotNum, record.ToBytes())
}

// NumRecords はレコード数を取得する
func (ln *LeafNode) NumRecords() int {
	return ln.body.NumSlots()
}

// CanTransferRecord は兄弟ノードにレコードを転送できるか判定する
//   - toRight: true の場合は右の兄弟に転送する
//   - return: 転送後も半分以上埋まっている場合は true を返す
func (ln *LeafNode) CanTransferRecord(toRight bool) bool {
	if ln.NumRecords() <= 1 {
		return false
	}

	// 右の兄弟に転送する場合は末尾レコード、左の兄弟に転送する場合は先頭レコードが転送対象
	var targetIndex int
	if toRight {
		targetIndex = ln.NumRecords() - 1
	}
	targetRecordData := ln.body.Cell(targetIndex)
	targetRecordSize := len(targetRecordData)

	freeSpaceAfterTransfer := ln.body.FreeSpace() + targetRecordSize + pointerSize
	return 2*freeSpaceAfterTransfer < ln.body.Capacity()
}

// Record は指定されたスロット番号のレコードを取得する
func (ln *LeafNode) Record(slotNum int) Record {
	return recordFromBytes(ln.body.Cell(slotNum))
}

// SearchSlotNum は指定された key に対応するスロット番号を検索する
//   - 見つかった場合: (スロット番号, true)
//   - 見つからなかった場合: (0, false)
func (ln *LeafNode) SearchSlotNum(key []byte) (int, bool) {
	return binarySearch(ln, key)
}

// PrevPageId は前のリーフノードのページ ID を取得する
func (ln *LeafNode) PrevPageId() page.PageId {
	return page.ReadPageId(ln.header[nodeHeaderSize:], 0)
}

// NextPageId は次のリーフノードのページ ID を取得する
func (ln *LeafNode) NextPageId() page.PageId {
	return page.ReadPageId(ln.header[nodeHeaderSize:], 8)
}

// SetPrevPageId は前のリーフノードのページ ID を設定する
func (ln *LeafNode) SetPrevPageId(prevPageId page.PageId) {
	prevPageId.WriteTo(ln.header[nodeHeaderSize:], 0)
}

// SetNextPageId は次のリーフノードのページ ID を設定する
func (ln *LeafNode) SetNextPageId(nextPageId page.PageId) {
	nextPageId.WriteTo(ln.header[nodeHeaderSize:], 8)
}

// TransferAllFrom は src のすべてのレコードを自分の末尾に転送する (src のレコードはすべて削除される)
func (ln *LeafNode) TransferAllFrom(src *LeafNode) bool {
	return src.body.TransferAllTo(ln.body)
}

// IsHalfFull はリーフノードが半分以上埋まっているかどうかを判定する
func (ln *LeafNode) IsHalfFull() bool {
	return 2*ln.body.FreeSpace() < ln.body.Capacity()
}

// maxRecordSize は自身のノードに格納できる最大のレコードサイズを返す
func (ln *LeafNode) maxRecordSize() int {
	// /2: ノード分割時に各ノードが半分以上埋まることを保証するため、1 レコードは容量の半分以下でなければならない
	// -pointerSize: Slotted Page ではレコードごとに pointer が必要なため、その分を差し引く
	return ln.body.Capacity()/2 - pointerSize
}

// transfer は先頭のレコードを別のリーフノードに移動する
func (ln *LeafNode) transfer(dest *LeafNode) error {
	nextIndex := dest.NumRecords()
	data := ln.body.Cell(0)

	if !dest.body.Insert(nextIndex, data) {
		return errors.New("no space in dest leaf node")
	}

	ln.body.Remove(0)
	return nil
}
