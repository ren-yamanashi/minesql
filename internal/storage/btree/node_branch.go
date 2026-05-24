package btree

import (
	"bytes"
	"errors"

	"github.com/ren-yamanashi/minesql/internal/storage/page"
)

// ブランチノード内のオフセット
const (
	branchNodeRightChildOffset = 0
	branchNodeHeaderSize       = 8
)

type branchNode struct {
	// ノードタイプヘッダー + ブランチノードヘッダー
	//   - header[0:8]: ノードタイプ
	//   - header[8:16]: 右の子の PageId
	header []byte
	body   *slottedPage
}

func newBranchNode(pg *page.Page) *branchNode {
	data := pg.Body
	headerSize := nodeHeaderSize + branchNodeHeaderSize
	header := data[:headerSize]
	body := newSlottedPage(data[headerSize:])
	return &branchNode{
		header: header,
		body:   body,
	}
}

// initialize はブランチノードを初期化する (初期化時のレコード数は 1)
//   - key: 最初のレコードのキー
//   - leftChildPageId: 最初のレコードの非キーフィールド (左の子の PageId)
//   - rightChildId: ヘッダーに設定する右の子の PageId
func (bn *branchNode) initialize(key []byte, leftChildPageId, rightChildId page.Id) error {
	copy(bn.header[:nodeHeaderSize], nodeTypeBranch)
	bn.body.initialize()

	record := NewRecord([]byte{}, key, leftChildPageId.ToBytes())
	if !bn.insert(0, record) {
		return errors.New("new branch node must have space")
	}

	rightChildId.WriteTo(bn.header[nodeHeaderSize:], branchNodeRightChildOffset)
	return nil
}

// insert はレコードを挿入する
//   - slotNum: 挿入先のスロット番号
//   - record: 挿入するレコード
//   - return: 挿入に成功した場合は true
func (bn *branchNode) insert(slotNum int, record Record) bool {
	recordBytes := record.ToBytes()
	if len(recordBytes) > bn.maxRecordSize() {
		return false
	}
	return bn.body.insert(slotNum, recordBytes)
}

// splitInsert はブランチノードを分割しながらレコードを挿入する
//   - newBranch: 分割後の新しいブランチノード
//   - newRecord: 挿入するレコード
//   - return: 新しいブランチノードの最小キー
func (bn *branchNode) splitInsert(newBranch *branchNode, newRecord Record) ([]byte, error) {
	copy(newBranch.header[:nodeHeaderSize], nodeTypeBranch)
	newBranch.body.initialize()
	for {
		// newBranch が十分に埋まったら、挿入対象のレコードを古いノードに挿入
		boundaryKey, ok, err := newBranch.tryExtractBoundaryKey()
		if err != nil {
			return nil, err
		}
		if ok {
			slotNum, _ := bn.searchSlotNum(newRecord.Key())
			if !bn.insert(slotNum, newRecord) {
				return nil, errors.New("old branch node must have space")
			}
			return boundaryKey, nil
		}

		// `古いノードの先頭レコードのキー < 挿入対象のキー` の場合
		if bn.record(0).compareKey(newRecord.Key()) < 0 {
			if err := bn.transfer(newBranch); err != nil {
				return nil, err
			}
			continue
		}

		// `古いノードの先頭レコードのキー >= 挿入対象のキー` の場合
		if !newBranch.insert(newBranch.numRecords(), newRecord) {
			return nil, errors.New("new branch node must have space")
		}
		for {
			boundaryKey, ok, err := newBranch.tryExtractBoundaryKey()
			if err != nil {
				return nil, err
			}
			if ok {
				return boundaryKey, nil
			}
			if err := bn.transfer(newBranch); err != nil {
				return nil, err
			}
		}
	}
}

// delete はレコードを削除する
func (bn *branchNode) delete(slotNum int) {
	bn.body.delete(slotNum)
}

// update は指定されたスロットのレコードを更新する
//   - slotNum: 更新するレコードのスロット番号
//   - record: 新しいレコード
func (bn *branchNode) update(slotNum int, record Record) bool {
	return bn.body.update(slotNum, record.ToBytes())
}

// numRecords はレコード数を取得する
func (bn *branchNode) numRecords() int {
	return bn.body.numSlots()
}

// canTransferRecord は兄弟ノードにレコードを転送できるか判定する
//   - isLeftSibling: true の場合は左の兄弟からの転送 (末尾レコードが転送対象)
//   - return: 転送後も半分以上埋まっている場合は true を返す
func (bn *branchNode) canTransferRecord(isLeftSibling bool) bool {
	if bn.numRecords() <= 1 {
		return false
	}

	// 左の兄弟の場合は末尾レコード、右の兄弟の場合は先頭レコードが転送対象
	var targetIndex int
	if isLeftSibling {
		targetIndex = bn.numRecords() - 1
	}
	targetRecordData := bn.body.cell(targetIndex)
	targetRecordSize := len(targetRecordData)

	freeSpaceAfterTransfer := bn.body.freeSpace() + targetRecordSize + slottedPagePointerSize
	return 2*freeSpaceAfterTransfer < bn.body.capacity()
}

// record は指定されたスロット番号のレコードを取得する
func (bn *branchNode) record(slotNum int) Record {
	return recordFromBytes(bn.body.cell(slotNum))
}

// searchSlotNum は指定された key に対応するスロット番号を検索する
//   - 見つかった場合: (スロット番号, true)
//   - 見つからなかった場合: (挿入すべき位置, false)
func (bn *branchNode) searchSlotNum(key []byte) (int, bool) {
	return binarySearch(bn, key)
}

// childPageId は指定された slotNum に対応する子ページの PageId を取得する
func (bn *branchNode) childPageId(slotNum int) (page.Id, error) {
	if slotNum == bn.numRecords() {
		return page.ReadId(bn.header[nodeHeaderSize:], branchNodeRightChildOffset), nil
	}
	record := bn.record(slotNum)
	return page.RestoreId(record.NonKey())
}

// rightChildPageId は右端の子の PageId を取得する
func (bn *branchNode) rightChildPageId() page.Id {
	return page.ReadId(bn.header[nodeHeaderSize:], branchNodeRightChildOffset)
}

// setRightChildPageId は右端の子の PageId を設定する
func (bn *branchNode) setRightChildPageId(pageId page.Id) {
	pageId.WriteTo(bn.header[nodeHeaderSize:], branchNodeRightChildOffset)
}

// transferAllFrom は src のすべてのレコードを自分の末尾に転送する (src のレコードはすべて削除される)
func (bn *branchNode) transferAllFrom(src *branchNode) bool {
	return src.body.transferAllTo(bn.body)
}

// isHalfFull はブランチノードが半分以上埋まっているかどうかを判定する
func (bn *branchNode) isHalfFull() bool {
	return 2*bn.body.freeSpace() < bn.body.capacity()
}

// fillRightChild は右端の子の PageId を設定し、最後のレコードキーを返す (右端のレコードは削除される)
//   - return: 取り出したキー
func (bn *branchNode) fillRightChild() ([]byte, error) {
	lastSlotNum := bn.numRecords() - 1
	record := bn.record(lastSlotNum)
	rightChild, err := page.RestoreId(record.NonKey())
	if err != nil {
		return nil, err
	}

	key := bytes.Clone(record.Key())
	bn.body.delete(lastSlotNum)
	rightChild.WriteTo(bn.header[nodeHeaderSize:], branchNodeRightChildOffset)
	return key, nil
}

// tryExtractBoundaryKey は末尾レコードを親ノードに伝播させる境界キーとして取り出せるか判定し、可能なら取り出す
//   - return:
//   - 取り出し後も半分以上の充填率を維持できる場合: 境界キー, true
//   - 維持できない場合: nil, false
func (bn *branchNode) tryExtractBoundaryKey() ([]byte, bool, error) {
	if bn.numRecords() < 2 {
		return nil, false, nil
	}

	lastRecordSize := len(bn.body.cell(bn.numRecords() - 1))
	freeSpaceAfter := bn.body.freeSpace() + lastRecordSize + slottedPagePointerSize
	if 2*freeSpaceAfter >= bn.body.capacity() {
		return nil, false, nil
	}

	key, err := bn.fillRightChild()
	if err != nil {
		return nil, false, err
	}
	return key, true, nil
}

// maxRecordSize は自身のノード内に格納できる最大のレコードサイズを返す
func (bn *branchNode) maxRecordSize() int {
	// /2: ノード分割時に各ノードが半分以上埋まることを保証するため、1 レコードは容量の半分以下でなければならない
	// -pointerSize: Slotted Page ではレコードごとに pointer が必要なため、その分を差し引く
	return bn.body.capacity()/2 - slottedPagePointerSize
}

// transfer は先頭のレコードを別のブランチノードに移動する
func (bn *branchNode) transfer(dest *branchNode) error {
	nextIndex := dest.numRecords()
	data := bn.body.cell(0)

	if !dest.body.insert(nextIndex, data) {
		return errors.New("no space in dest branch node")
	}

	bn.body.delete(0)
	return nil
}
