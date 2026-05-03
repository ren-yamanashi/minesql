package node

import (
	"bytes"
	"errors"

	"github.com/ren-yamanashi/minesql/internal/storage/page"
)

// ブランチノード内のオフセット
const (
	branchRightChildOffset = 0
	branchHeaderSize       = 8
)

type BranchNode struct {
	// ノードタイプヘッダー + ブランチノードヘッダー
	//   - header[0:8]: ノードタイプ
	//   - header[8:16]: 右の子の PageId
	header []byte
	body   *SlottedPage
}

func NewBranchNode(pg *page.Page) *BranchNode {
	data := pg.Body
	copy(data[0:8], NodeTypeBranch)
	headerSize := nodeHeaderSize + branchHeaderSize
	header := data[:headerSize]
	body := NewSlottedPage(data[headerSize:])
	return &BranchNode{
		header: header,
		body:   body,
	}
}

// Initialize はブランチノードを初期化する (初期化時のレコード数は 1)
//   - key: 最初のレコードのキー
//   - leftChildPageId: 最初のレコードの非キーフィールド (左の子の PageId)
//   - rightChildId: ヘッダーに設定する右の子の PageId
func (bn *BranchNode) Initialize(key []byte, leftChildPageId, rightChildId page.PageId) error {
	bn.body.Initialize()

	record := NewRecord([]byte{}, key, leftChildPageId.ToBytes())
	if !bn.Insert(0, record) {
		return errors.New("new branch node must have space")
	}

	rightChildId.WriteTo(bn.header[nodeHeaderSize:], branchRightChildOffset)
	return nil
}

// Insert はレコードを挿入する
//   - slotNum: 挿入先のスロット番号
//   - record: 挿入するレコード
//   - return: 挿入に成功した場合は true
func (bn *BranchNode) Insert(slotNum int, record Record) bool {
	recordBytes := record.ToBytes()
	if len(recordBytes) > bn.maxRecordSize() {
		return false
	}
	return bn.body.Insert(slotNum, recordBytes)
}

// SplitInsert はブランチノードを分割しながらレコードを挿入する
//   - newBranch: 分割後の新しいブランチノード
//   - newRecord: 挿入するレコード
//   - return: 新しいブランチノードの最小キー
func (bn *BranchNode) SplitInsert(newBranch *BranchNode, newRecord Record) ([]byte, error) {
	newBranch.body.Initialize()
	for {
		// newBranch が十分に埋まったら、挿入対象のレコードを古いノードに挿入
		boundaryKey, ok, err := newBranch.tryExtractBoundaryKey()
		if err != nil {
			return nil, err
		}
		if ok {
			slotNum, _ := bn.SearchSlotNum(newRecord.Key())
			if !bn.Insert(slotNum, newRecord) {
				return nil, errors.New("old branch node must have space")
			}
			return boundaryKey, nil
		}

		// `古いノードの先頭レコードのキー < 挿入対象のキー` の場合
		if bn.Record(0).CompareKey(newRecord.Key()) < 0 {
			if err := bn.transfer(newBranch); err != nil {
				return nil, err
			}
			continue
		}

		// `古いノードの先頭レコードのキー >= 挿入対象のキー` の場合
		if !newBranch.Insert(newBranch.NumRecords(), newRecord) {
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

// Delete はレコードを削除する
func (bn *BranchNode) Delete(slotNum int) {
	bn.body.Delete(slotNum)
}

// Update は指定されたスロットのレコードを更新する
//   - slotNum: 更新するレコードのスロット番号
//   - record: 新しいレコード
func (bn *BranchNode) Update(slotNum int, record Record) bool {
	return bn.body.Update(slotNum, record.ToBytes())
}

// NumRecords はレコード数を取得する
func (bn *BranchNode) NumRecords() int {
	return bn.body.NumSlots()
}

// CanTransferRecord は兄弟ノードにレコードを転送できるか判定する
//   - toRight: true の場合は右の兄弟に転送する
//   - return: 転送後も半分以上埋まっている場合は true を返す
func (bn *BranchNode) CanTransferRecord(toRight bool) bool {
	if bn.NumRecords() <= 1 {
		return false
	}

	// 右の兄弟に転送する場合は末尾レコード、左の兄弟に転送する場合は先頭レコードが転送対象
	var targetIndex int
	if toRight {
		targetIndex = bn.NumRecords() - 1
	}
	targetRecordData := bn.body.Cell(targetIndex)
	targetRecordSize := len(targetRecordData)

	freeSpaceAfterTransfer := bn.body.FreeSpace() + targetRecordSize + pointerSize
	return 2*freeSpaceAfterTransfer < bn.body.Capacity()
}

// Record は指定されたスロット番号のレコードを取得する
func (bn *BranchNode) Record(slotNum int) Record {
	return recordFromBytes(bn.body.Cell(slotNum))
}

// SearchSlotNum は指定された key に対応するスロット番号を検索する
//   - 見つかった場合: (スロット番号, true)
//   - 見つからなかった場合: (0, false)
func (bn *BranchNode) SearchSlotNum(key []byte) (int, bool) {
	return binarySearch(bn, key)
}

// ChildPageId は指定された slotNum に対応する子ページの PageId を取得する
func (bn *BranchNode) ChildPageId(slotNum int) (page.PageId, error) {
	if slotNum == bn.NumRecords() {
		return page.ReadPageId(bn.header[nodeHeaderSize:], branchRightChildOffset), nil
	}
	record := bn.Record(slotNum)
	return page.RestorePageId(record.NonKey())
}

// RightChildPageId は右端の子の PageId を取得する
func (bn *BranchNode) RightChildPageId() page.PageId {
	return page.ReadPageId(bn.header[nodeHeaderSize:], branchRightChildOffset)
}

// SetRightChildPageId は右端の子の PageId を設定する
func (bn *BranchNode) SetRightChildPageId(pageId page.PageId) {
	pageId.WriteTo(bn.header[nodeHeaderSize:], branchRightChildOffset)
}

// TransferAllFrom は src のすべてのレコードを自分の末尾に転送する (src のレコードはすべて削除される)
func (bn *BranchNode) TransferAllFrom(src *BranchNode) bool {
	return src.body.TransferAllTo(bn.body)
}

// IsHalfFull はブランチノードが半分以上埋まっているかどうかを判定する
func (bn *BranchNode) IsHalfFull() bool {
	return 2*bn.body.FreeSpace() < bn.body.Capacity()
}

// fillRightChild は右端の子の PageId を設定し、最後のレコードキーを返す (右端のレコードは削除される)
//   - return: 取り出したキー
func (bn *BranchNode) fillRightChild() ([]byte, error) {
	lastSlotNum := bn.NumRecords() - 1
	record := bn.Record(lastSlotNum)
	rightChild, err := page.RestorePageId(record.NonKey())
	if err != nil {
		return nil, err
	}

	key := bytes.Clone(record.Key())
	bn.body.Delete(lastSlotNum)
	rightChild.WriteTo(bn.header[nodeHeaderSize:], branchRightChildOffset)
	return key, nil
}

// tryExtractBoundaryKey は末尾レコードを親ノードに伝播させる境界キーとして取り出せるか判定し、可能なら取り出す
//   - return:
//   - 取り出し後も半分以上の充填率を維持できる場合: 境界キー, true
//   - 維持できない場合: nil, false
func (bn *BranchNode) tryExtractBoundaryKey() ([]byte, bool, error) {
	if bn.NumRecords() < 2 {
		return nil, false, nil
	}

	lastRecordSize := len(bn.body.Cell(bn.NumRecords() - 1))
	freeSpaceAfter := bn.body.FreeSpace() + lastRecordSize + pointerSize
	if 2*freeSpaceAfter >= bn.body.Capacity() {
		return nil, false, nil
	}

	key, err := bn.fillRightChild()
	if err != nil {
		return nil, false, err
	}
	return key, true, nil
}

// maxRecordSize は自身のノード内に格納できる最大のレコードサイズを返す
func (bn *BranchNode) maxRecordSize() int {
	// /2: ノード分割時に各ノードが半分以上埋まることを保証するため、1 レコードは容量の半分以下でなければならない
	// -pointerSize: Slotted Page ではレコードごとに pointer が必要なため、その分を差し引く
	return bn.body.Capacity()/2 - pointerSize
}

// transfer は先頭のレコードを別のブランチノードに移動する
func (bn *BranchNode) transfer(dest *BranchNode) error {
	nextIndex := dest.NumRecords()
	data := bn.body.Cell(0)

	if !dest.body.Insert(nextIndex, data) {
		return errors.New("no space in dest branch node")
	}

	bn.body.Delete(0)
	return nil
}
