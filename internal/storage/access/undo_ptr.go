package access

import (
	"encoding/binary"
	"errors"
)

const UndoPtrSize = 4 // PageNumber (2B) + Offset (2B)

var (
	// NullUndoPtr は前バージョンが存在しないことを示すセンチネル値
	//
	// UndoPtr{0, 0} は undo ファイルの最初のページの先頭レコードを指す有効なポインタであるため、ゼロ値を null として使えない
	NullUndoPtr         = UndoPtr{PageNumber: 0xFFFF, Offset: 0xFFFF}
	ErrInvalidUndoPtrData = errors.New("data size must be at least 4 bytes to decode UndoPtr")
)

// UndoPtr は undo ログレコードの位置を指すポインタ
type UndoPtr struct {
	PageNumber uint16 // undo ページのページ番号
	Offset     uint16 // undo ページ内のバイトオフセット
}

// IsNull は前バージョンが存在しないことを示すセンチネル値かどうかを返す
func (p UndoPtr) IsNull() bool {
	return p == NullUndoPtr
}

// Encode は UndoPtr を 4 バイトのバイト列にエンコードする
func (p UndoPtr) Encode() []byte {
	buf := make([]byte, UndoPtrSize)
	binary.BigEndian.PutUint16(buf[0:2], p.PageNumber)
	binary.BigEndian.PutUint16(buf[2:4], p.Offset)
	return buf
}

// DecodeUndoPtr はバイト列から UndoPtr をデコードする
func DecodeUndoPtr(data []byte) (UndoPtr, error) {
	if len(data) < UndoPtrSize {
		return NullUndoPtr, ErrInvalidUndoPtrData
	}
	return UndoPtr{
		PageNumber: binary.BigEndian.Uint16(data[0:2]),
		Offset:     binary.BigEndian.Uint16(data[2:4]),
	}, nil
}
