package access

import (
	"encoding/binary"

	"github.com/ren-yamanashi/minesql/internal/storage/lock"
)

const lastModifiedSize = 8

// encodeRecordNonKeyPrefix は lastModified と rollPtr を Non-key 領域の先頭に付与するバイト列を返す
func encodeRecordNonKeyPrefix(lastModified lock.TrxId, rollPtr UndoPtr) []byte {
	buf := make([]byte, lastModifiedSize+UndoPtrSize)
	binary.BigEndian.PutUint64(buf[0:lastModifiedSize], lastModified)
	copy(buf[lastModifiedSize:lastModifiedSize+UndoPtrSize], rollPtr.Encode())
	return buf
}

// decodeRecordNonKey は B+Tree レコードの Non-key バイト列から lastModified, rollPtr, 非キーカラムを分離する
func decodeRecordNonKey(nonKeyBytes []byte) (lastModified lock.TrxId, rollPtr UndoPtr, nonKeyColumns []byte) {
	lastModified = binary.BigEndian.Uint64(nonKeyBytes[0:lastModifiedSize])
	rollPtr, _ = DecodeUndoPtr(nonKeyBytes[lastModifiedSize : lastModifiedSize+UndoPtrSize])
	nonKeyColumns = nonKeyBytes[lastModifiedSize+UndoPtrSize:]
	return
}
