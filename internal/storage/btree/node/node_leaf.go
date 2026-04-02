package node

import (
	"errors"
	"minesql/internal/storage/page"
)

const leafHeaderSize = 16

type Leaf struct {
	data []byte       // ページデータ全体 (ノードタイプヘッダー + リーフノードヘッダー + Slotted Page のボディ)
	body *SlottedPage // Slotted Page のボディ部分
}

// NewLeaf はページデータを受け取ってそのデータをリーフノードとして扱うための構造体を返す
//   - data: ページデータ全体
//
// 引数の data はリーフノードとして以下の構成で扱われる
//   - data[0:8]: ノードタイプ
//   - data[8:16]: 前ページ ID
//   - data[16:24]: 次ページ ID
//   - data[24:]: Slotted Page (24 = headerSize + leafHeaderSize)
func NewLeaf(data []byte) *Leaf {
	// ノードタイプを設定
	copy(data[0:8], NODE_TYPE_LEAF)

	// data[24:] 以降を Slotted Page のボディとして扱う
	body := NewSlottedPage(data[headerSize+leafHeaderSize:])

	return &Leaf{
		data: data,
		body: body,
	}
}

// Initialize はリーフノードを初期化する
//
// 初期化時には、前後のリーフノードのポインタ (ページ ID) には無効値が設定される
func (ln *Leaf) Initialize() {
	page.INVALID_PAGE_ID.WriteTo(ln.Body(), 0) // 初期化時には、前のページ ID を無効値に設定
	page.INVALID_PAGE_ID.WriteTo(ln.Body(), 8) // 初期化時には、次のページ ID を無効値に設定
	ln.body.Initialize()
}

// Insert はレコードを挿入する
//   - slotNum: 挿入先のスロット番号 (slotted page のスロット番号)
//   - record: 挿入するレコード
//   - 戻り値: 挿入に成功したかどうか
func (ln *Leaf) Insert(slotNum int, record Record) bool {
	recordBytes := record.ToBytes()

	if len(recordBytes) > ln.maxRecordSize() {
		return false
	}

	return ln.body.Insert(slotNum, recordBytes)
}

// SplitInsert はリーフノードを分割しながらレコードを挿入する
//   - newLeaf: 分割後の新しいリーフノード
//   - newRecord: 挿入するレコード
//   - 戻り値: 新しいリーフノードの最小キー
func (ln *Leaf) SplitInsert(newLeaf *Leaf, newRecord Record) ([]byte, error) {
	newLeaf.Initialize()

	for {
		if newLeaf.IsHalfFull() {
			slotNum, _ := ln.SearchSlotNum(newRecord.KeyBytes())
			if !ln.Insert(slotNum, newRecord) {
				return nil, errors.New("old leaf must have space")
			}
			break
		}

		// "古いノードの先頭 (スロット番号=0) のレコードのキー < 新しいレコードのキー" の場合
		// レコードを新しいリーフノードに移動する
		if ln.RecordAt(0).CompareKey(newRecord.KeyBytes()) < 0 {
			err := ln.transfer(newLeaf)
			if err != nil {
				return nil, err
			}
		} else {
			// 新しいレコードを新しいリーフノードに挿入し、残りのレコードを新しいリーフノードに移動する
			newLeaf.Insert(newLeaf.NumRecords(), newRecord)
			for !newLeaf.IsHalfFull() {
				err := ln.transfer(newLeaf)
				if err != nil {
					return nil, err
				}
			}
			break
		}
	}

	return newLeaf.RecordAt(0).KeyBytes(), nil
}

// Delete はレコードを削除する
//   - slotNum: 削除するレコードのスロット番号 (slotted page のスロット番号)
func (ln *Leaf) Delete(slotNum int) {
	ln.body.Remove(slotNum)
}

// Update は指定されたスロットのレコードを更新する
//   - slotNum: 更新するレコードのスロット番号
//   - record: 新しいレコード (key は変更されない前提)
//   - 戻り値: 更新に成功したかどうか (空き容量不足の場合は false)
func (ln *Leaf) Update(slotNum int, record Record) bool {
	return ln.body.Update(slotNum, record.ToBytes())
}

// CanTransferRecord は兄弟ノードにレコードを転送できるかどうかを判定する
//
// 転送後も半分以上埋まっている場合は true を返す
//
//   - toRight: true の場合は右の兄弟に転送 (末尾レコードを転送)、false の場合は左の兄弟に転送 (先頭レコードを転送)
func (ln *Leaf) CanTransferRecord(toRight bool) bool {
	if ln.NumRecords() <= 1 {
		return false
	}

	// 右の兄弟に転送する場合は末尾レコード、左の兄弟に転送する場合は先頭レコードを転送対象とする
	targetIndex := 0
	if toRight {
		targetIndex = ln.NumRecords() - 1
	}
	targetRecordData := ln.body.Data(targetIndex)
	targetRecordSize := len(targetRecordData)

	// 転送後の空き容量を計算
	freeSpaceAfterTransfer := ln.body.FreeSpace() + targetRecordSize + 4 // 4 はポインタサイズ

	// 転送後の空き容量が、ノード全体の容量の半分未満であれば (転送後も半分以上埋まっていると判断できるので) 転送可能と判断する
	return 2*freeSpaceAfterTransfer < ln.body.Capacity()
}

// Body はノードタイプヘッダーを除いたボディ部分を取得する (リーフノードヘッダー + Slotted Page のボディ)
func (ln *Leaf) Body() []byte {
	return ln.data[headerSize:]
}

// NumRecords はレコード数を取得する
func (ln *Leaf) NumRecords() int {
	return ln.body.NumSlots()
}

// RecordAt は指定されたスロット番号のレコードを取得する
//   - slotNum: slotted page のスロット番号
func (ln *Leaf) RecordAt(slotNum int) Record {
	data := ln.body.Data(slotNum)
	return recordFromBytes(data)
}

// SearchSlotNum はキーから、対応するスロット番号 (slotted page のスロット番号) を検索する (二分探索)
//   - 見つかった場合: (スロット番号, true)
//   - 見つからなかった場合: (0, false)
func (ln *Leaf) SearchSlotNum(key []byte) (int, bool) {
	return binarySearch(ln, key)
}

// PrevPageId は前のリーフノードのページ ID を取得する
//
// 前のリーフノードが存在しない場合は nil を返す
func (ln *Leaf) PrevPageId() *page.PageId {
	pageId := page.ReadPageIdFromPageData(ln.Body(), 0)
	if pageId.IsInvalid() {
		return nil
	}
	return &pageId
}

// NextPageId は次のリーフノードのページ ID を取得する
//
// 次のリーフノードが存在しない場合は nil を返す
func (ln *Leaf) NextPageId() *page.PageId {
	pageId := page.ReadPageIdFromPageData(ln.Body(), 8)
	if pageId.IsInvalid() {
		return nil
	}
	return &pageId
}

// SetPrevPageId は前のリーフノードのページ ID を設定する
//   - prevPageId: 前のリーフノードのページ ID (前のリーフノードが存在しない場合は nil を指定する)
func (ln *Leaf) SetPrevPageId(prevPageId *page.PageId) {
	var pageId page.PageId
	if prevPageId == nil {
		pageId = page.INVALID_PAGE_ID
	} else {
		pageId = *prevPageId
	}
	pageId.WriteTo(ln.Body(), 0)
}

// SetNextPageId は次のリーフノードのページ ID を設定する
//   - nextPageId: 次のリーフノードのページ ID (次のリーフノードが存在しない場合は nil を指定する)
func (ln *Leaf) SetNextPageId(nextPageId *page.PageId) {
	var pageId page.PageId
	if nextPageId == nil {
		pageId = page.INVALID_PAGE_ID
	} else {
		pageId = *nextPageId
	}
	pageId.WriteTo(ln.Body(), 8)
}

// TransferAllFrom は src のすべてのレコードを自分の末尾に転送する (src のレコードはすべて削除される)
//
// 空き容量不足で転送できない場合は false を返す (src のデータはそのまま保持される)
func (ln *Leaf) TransferAllFrom(src *Leaf) bool {
	return src.body.TransferAllTo(ln.body)
}

// IsHalfFull はリーフノードが半分以上埋まっているかどうかを判定する
func (ln *Leaf) IsHalfFull() bool {
	return 2*ln.body.FreeSpace() < ln.body.Capacity()
}

// maxRecordSize はリーフノード内の最大レコードサイズを取得する
func (ln *Leaf) maxRecordSize() int {
	// /2: ノード分割時に各ノードが半分以上埋まることを保証するため、1 レコードは容量の半分以下でなければならない
	// -4: Slotted Page ではレコードごとに 4 バイトのスロットポインタ (offset 2B + size 2B) が必要なため、その分を差し引く
	return ln.body.Capacity()/2 - 4
}

// transfer は先頭のレコードを別のリーフノードに移動する
func (ln *Leaf) transfer(dest *Leaf) error {
	nextIndex := dest.NumRecords()
	data := ln.body.Data(0)

	if !dest.body.Insert(nextIndex, data) {
		return errors.New("no space in dest leaf")
	}

	ln.body.Remove(0)
	return nil
}
