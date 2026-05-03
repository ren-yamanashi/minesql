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
	nodeBody
}

func NewBranchNode(pg *page.Page) *BranchNode {
	data := pg.Body
	copy(data[0:8], NodeTypeBranch)
	headerSize := nodeHeaderSize + branchHeaderSize
	header := data[:headerSize]
	body := NewSlottedPage(data[headerSize:])
	return &BranchNode{
		header:   header,
		nodeBody: nodeBody{body: body},
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
			if err := bn.transfer(&newBranch.nodeBody); err != nil {
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
			if err := bn.transfer(&newBranch.nodeBody); err != nil {
				return nil, err
			}
		}
	}
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
