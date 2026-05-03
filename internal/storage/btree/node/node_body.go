package node

import "errors"

// nodeBody は LeafNode と BranchNode の共通の SlottedPage 操作をまとめた構造体
type nodeBody struct {
	body *SlottedPage
}

// Insert はレコードを挿入する
//   - slotNum: 挿入先のスロット番号
//   - record: 挿入するレコード
//   - return: 挿入に成功した場合は true
func (nb *nodeBody) Insert(slotNum int, record Record) bool {
	recordBytes := record.ToBytes()
	if len(recordBytes) > nb.maxRecordSize() {
		return false
	}
	return nb.body.Insert(slotNum, recordBytes)
}

// Delete はレコードを削除する
func (nb *nodeBody) Delete(slotNum int) {
	nb.body.Delete(slotNum)
}

// Update は指定されたスロットのレコードを更新する
//   - slotNum: 更新するレコードのスロット番号
//   - record: 新しいレコード (key は変更されない前提)
func (nb *nodeBody) Update(slotNum int, record Record) bool {
	return nb.body.Update(slotNum, record.ToBytes())
}

// NumRecords はレコード数を取得する
func (nb *nodeBody) NumRecords() int {
	return nb.body.NumSlots()
}

// CanTransferRecord は兄弟ノードにレコードを転送できるか判定する
//   - toRight: true の場合は右の兄弟に転送する
//   - return: 転送後も半分以上埋まっている場合は true を返す
func (nb *nodeBody) CanTransferRecord(toRight bool) bool {
	if nb.NumRecords() <= 1 {
		return false
	}

	// 右の兄弟に転送する場合は末尾レコード、左の兄弟に転送する場合は先頭レコードが転送対象
	var targetIndex int
	if toRight {
		targetIndex = nb.NumRecords() - 1
	}
	targetRecordData := nb.body.Cell(targetIndex)
	targetRecordSize := len(targetRecordData)

	freeSpaceAfterTransfer := nb.body.FreeSpace() + targetRecordSize + pointerSize
	return 2*freeSpaceAfterTransfer < nb.body.Capacity()
}

// Record は指定されたスロット番号のレコードを取得する
func (nb *nodeBody) Record(slotNum int) Record {
	return recordFromBytes(nb.body.Cell(slotNum))
}

// SearchSlotNum は指定された key に対応するスロット番号を検索する
//   - 見つかった場合: (スロット番号, true)
//   - 見つからなかった場合: (0, false)
func (nb *nodeBody) SearchSlotNum(key []byte) (int, bool) {
	return binarySearch(nb, key)
}

// IsHalfFull はノードが半分以上埋まっているかどうかを判定する
func (nb *nodeBody) IsHalfFull() bool {
	return 2*nb.body.FreeSpace() < nb.body.Capacity()
}

// maxRecordSize は自身のノードに格納できる最大のレコードサイズを返す
func (nb *nodeBody) maxRecordSize() int {
	// /2: ノード分割時に各ノードが半分以上埋まることを保証するため、1 レコードは容量の半分以下でなければならない
	// -pointerSize: Slotted Page ではレコードごとに pointer が必要なため、その分を差し引く
	return nb.body.Capacity()/2 - pointerSize
}

// transfer は先頭のレコードを別のノードに移動する
func (nb *nodeBody) transfer(dest *nodeBody) error {
	nextIndex := dest.NumRecords()
	data := nb.body.Cell(0)

	if !dest.body.Insert(nextIndex, data) {
		return errors.New("no space in dest node")
	}

	nb.body.Delete(0)
	return nil
}
