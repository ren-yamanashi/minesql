package page

// SlotPosition はページ内のレコードの物理的な位置を表す
type SlotPosition struct {
	PageId  PageId
	SlotNum int
}
