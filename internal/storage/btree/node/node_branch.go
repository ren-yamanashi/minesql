package node

import (
	"errors"
	"minesql/internal/storage/page"
)

const branchHeaderSize = 8

type Branch struct {
	data []byte       // ページデータ全体 (ノードタイプヘッダー + ブランチノードヘッダー + Slotted Page のボディ)
	body *SlottedPage // Slotted Page のボディ部分
}

// NewBranch はページデータを受け取ってそのデータをブランチノードとして扱うための構造体を返す
//   - data: ページデータ全体
//
// 引数の data はブランチノードとして以下の構成で扱われる
//   - data[0:8]: ノードタイプ
//   - data[8:16]: 右子ページ ID
//   - data[16:]: Slotted Page (16 = headerSize + branchHeaderSize)
func NewBranch(data []byte) *Branch {
	// ノードタイプを設定
	copy(data[0:8], NODE_TYPE_BRANCH)

	// data[16:] 以降を Slotted Page のボディとして扱う
	body := NewSlottedPage(data[headerSize+branchHeaderSize:])

	return &Branch{
		data: data,
		body: body,
	}
}

// Initialize はブランチノードを初期化する (初期化時には、レコード数は 1 つ)
//   - key: 最初のレコードのキー
//   - leftChildPageId: 最初のレコードの非キーフィールド (左の子ページのページ ID)
//   - rightChildPageId: ヘッダー部分に設定する右の子ページのページ ID
func (bn *Branch) Initialize(key []byte, leftChildPageId page.PageId, rightChildPageId page.PageId) error {
	bn.body.Initialize()

	// 左の子ページのポインタ (ページ ID) を非キーフィールドとした Record を作成
	record := NewRecord(nil, key, leftChildPageId.ToBytes())

	if !bn.Insert(0, record) {
		return errors.New("new branch must have space")
	}

	// ヘッダー部分に右の子ページのポインタ (ページ ID) を設定
	rightChildPageId.WriteTo(bn.Body(), 0)

	return nil
}

// Insert はレコードを挿入する
//   - slotNum: 挿入先のスロット番号 (slotted page のスロット番号)
//   - record: 挿入するレコード
//   - 戻り値: 挿入に成功したかどうか
func (bn *Branch) Insert(slotNum int, record Record) bool {
	recordBytes := record.ToBytes()

	if len(recordBytes) > bn.maxRecordSize() {
		return false
	}

	return bn.body.Insert(slotNum, recordBytes)
}

// SplitInsert はブランチノードを分割しながらレコードを挿入する
//   - newBranch: 分割後の新しいブランチノード
//   - newRecord: 挿入するレコード
//   - 戻り値: 新しいブランチノードの最小キー
func (bn *Branch) SplitInsert(newBranch *Branch, newRecord Record) ([]byte, error) {
	newBranch.body.Initialize()

	for {
		// newBranch が十分に埋まったら、末尾レコードを境界キーとして取り出す
		if boundaryKey, ok := newBranch.tryExtractBoundaryKey(); ok {
			slotNum, _ := bn.SearchSlotNum(newRecord.KeyBytes())
			if !bn.Insert(slotNum, newRecord) {
				return nil, errors.New("old branch must have space")
			}
			return boundaryKey, nil
		}

		// "古いノードの先頭 (スロット番号=0) のレコードのキー < 新しいレコードのキー" の場合
		// レコードを新しいブランチノードに移動する
		if bn.RecordAt(0).CompareKey(newRecord.KeyBytes()) < 0 {
			if err := bn.transfer(newBranch); err != nil {
				return nil, err
			}
		} else {
			// 新しいレコードを新しいブランチノードに挿入し、残りのレコードを新しいブランチノードに移動する
			newBranch.Insert(newBranch.NumRecords(), newRecord)
			for {
				if boundaryKey, ok := newBranch.tryExtractBoundaryKey(); ok {
					return boundaryKey, nil
				}
				if err := bn.transfer(newBranch); err != nil {
					return nil, err
				}
			}
		}
	}
}

// Delete はレコードを削除する
//   - slotNum: 削除するレコードのスロット番号 (slotted page のスロット番号)
func (bn *Branch) Delete(slotNum int) {
	bn.body.Remove(slotNum)
}

// Update は指定されたスロット番号のキーを更新する
//
// 子ノード側でレコードの追加・削除が起きて境界値 (最小キー) が変わった場合に、ブランチノード側のキーも更新する (ページ ID は変わらないので非キーフィールドはそのまま)
//
// 戻り値: 更新に成功したかどうか (空き容量不足の場合は false)
func (bn *Branch) Update(slotNum int, newKey []byte) bool {
	record := bn.RecordAt(slotNum)
	newRecord := NewRecord(record.HeaderBytes(), newKey, record.NonKeyBytes())
	return bn.body.Update(slotNum, newRecord.ToBytes())
}

// CanTransferRecord は兄弟ノードにレコードを転送できるかどうかを判定する
//
// 転送後も半分以上埋まっている場合は true を返す
//
// toRight: true の場合は右の兄弟に転送 (末尾レコードを転送)、false の場合は左の兄弟に転送 (先頭レコードを転送)
func (bn *Branch) CanTransferRecord(toRight bool) bool {
	if bn.NumRecords() <= 1 {
		return false
	}

	// 右の兄弟に転送する場合は末尾レコード、左の兄弟に転送する場合は先頭レコードを転送対象とする
	targetIndex := 0
	if toRight {
		targetIndex = bn.NumRecords() - 1
	}
	targetRecordData := bn.body.Data(targetIndex)
	targetRecordSize := len(targetRecordData)

	// 転送後の空き容量を計算
	freeSpaceAfterTransfer := bn.body.FreeSpace() + targetRecordSize + 4 // 4 はポインタサイズ

	// 転送後の空き容量が、ノード全体の容量の半分未満であれば (転送後も半分以上埋まっていると判断できるので) 転送可能と判断する
	return 2*freeSpaceAfterTransfer < bn.body.Capacity()
}

// Body はノードタイプヘッダーを除いたボディ部分を取得する (ブランチノードヘッダー + Slotted Page のボディ)
func (bn *Branch) Body() []byte {
	return bn.data[headerSize:]
}

// NumRecords はレコード数を取得する
func (bn *Branch) NumRecords() int {
	return bn.body.NumSlots()
}

// RecordAt は指定されたスロット番号のレコードを取得する
//
// slotNum: slotted page のスロット番号
func (bn *Branch) RecordAt(slotNum int) Record {
	data := bn.body.Data(slotNum)
	return recordFromBytes(data)
}

// SearchSlotNum はキーから、対応するスロット番号 (slotted page のスロット番号) を検索する (二分探索)
//
// 見つかった場合: (スロット番号, true)
//
// 見つからなかった場合: (0, false)
func (bn *Branch) SearchSlotNum(key []byte) (int, bool) {
	return binarySearch(bn, key)
}

// SearchChildSlotNum はキーから子ページのスロット番号を検索する
//
// キーが見つかった場合、そのキー以上の値は右側の子に進むため slotNum + 1 を返す
//
// キーが見つからない場合、挿入位置の左側の子に進むため slotNum をそのまま返す
func (bn *Branch) SearchChildSlotNum(key []byte) int {
	slotNum, found := bn.SearchSlotNum(key)
	if found {
		return slotNum + 1
	}
	return slotNum
}

// ChildPageIdAt は指定されたスロット番号の、子ページのページ ID を取得する
func (bn *Branch) ChildPageIdAt(slotNum int) page.PageId {
	if slotNum == bn.NumRecords() {
		// 右端の子ページ ID を返す
		return page.ReadPageIdFromPageData(bn.Body(), 0)
	}
	record := bn.RecordAt(slotNum)
	return page.RestorePageIdFromBytes(record.NonKeyBytes())
}

// RightChildPageId は右端の子ページ ID を取得する
func (bn *Branch) RightChildPageId() page.PageId {
	return page.ReadPageIdFromPageData(bn.Body(), 0)
}

// SetRightChildPageId は右端の子ページ ID を設定する
func (bn *Branch) SetRightChildPageId(pageId page.PageId) {
	pageId.WriteTo(bn.Body(), 0)
}

// TransferAllFrom は src のすべてのレコードを自分の末尾に転送する (src のレコードはすべて削除される)
func (bn *Branch) TransferAllFrom(src *Branch) {
	src.body.TransferAllTo(bn.body)
}

// IsHalfFull はブランチノードが半分以上埋まっているかどうかを判定する
func (bn *Branch) IsHalfFull() bool {
	return 2*bn.body.FreeSpace() < bn.body.Capacity()
}

// fillRightChild は右端の子ページ ID を設定し、最後のレコードのキーを返す (右端のレコードは削除される)
// 戻り値: 取り出したキー
func (bn *Branch) fillRightChild() []byte {
	lastId := bn.NumRecords() - 1
	record := bn.RecordAt(lastId)
	rightChild := page.RestorePageIdFromBytes(record.NonKeyBytes())
	key := make([]byte, len(record.KeyBytes()))

	// キーをコピー
	copy(key, record.KeyBytes())
	bn.body.Remove(lastId)

	// ブランチノードのヘッダー部分に右子ページ ID を設定
	rightChild.WriteTo(bn.Body(), 0)

	return key
}

// tryExtractBoundaryKey は末尾レコードを親ノードに伝播させる境界キーとして取り出せるか判定し、可能なら取り出す
//
// ブランチノードの分割では末尾レコードのキーが親ノードの境界値になる。
// 取り出し後も半分以上の充填率を維持できる場合のみ実行し、境界キーと true を返す。
// 維持できない場合は何もせず nil, false を返す。
func (bn *Branch) tryExtractBoundaryKey() ([]byte, bool) {
	if bn.NumRecords() < 2 {
		return nil, false
	}
	lastRecordSize := len(bn.body.Data(bn.NumRecords() - 1))
	freeSpaceAfter := bn.body.FreeSpace() + lastRecordSize + pointerSize
	if 2*freeSpaceAfter >= bn.body.Capacity() {
		return nil, false
	}
	return bn.fillRightChild(), true
}

// maxRecordSize は自身のノードに格納できる最大のレコードサイズを返す
func (bn *Branch) maxRecordSize() int {
	// /2: ノード分割時に各ノードが半分以上埋まることを保証するため、1 レコードは容量の半分以下でなければならない
	// -4: Slotted Page ではレコードごとに 4 バイトのスロットポインタ (offset 2B + size 2B) が必要なため、その分を差し引く
	return bn.body.Capacity()/2 - 4
}

// transfer は先頭のレコードを別のブランチノードに移動する
func (bn *Branch) transfer(dest *Branch) error {
	nextIndex := dest.NumRecords()
	data := bn.body.Data(0)

	if !dest.body.Insert(nextIndex, data) {
		return errors.New("no space in dest branch")
	}

	bn.body.Remove(0)
	return nil
}
