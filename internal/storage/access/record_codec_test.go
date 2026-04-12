package access

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestEncodeRecordNonKeyPrefix(t *testing.T) {
	t.Run("lastModified と rollPtr がエンコードされる", func(t *testing.T) {
		// GIVEN
		lastModified := TrxId(42)
		rollPtr := UndoPtr{PageNumber: 1, Offset: 100}

		// WHEN
		buf := encodeRecordNonKeyPrefix(lastModified, rollPtr)

		// THEN
		assert.Equal(t, 8+UndoPtrSize, len(buf))
	})

	t.Run("NullUndoPtr でもエンコードできる", func(t *testing.T) {
		// GIVEN
		lastModified := TrxId(1)
		rollPtr := NullUndoPtr

		// WHEN
		buf := encodeRecordNonKeyPrefix(lastModified, rollPtr)

		// THEN
		assert.Equal(t, 8+UndoPtrSize, len(buf))
	})
}

func TestDecodeRecordNonKey(t *testing.T) {
	t.Run("エンコードした lastModified と rollPtr をデコードできる", func(t *testing.T) {
		// GIVEN
		lastModified := TrxId(42)
		rollPtr := UndoPtr{PageNumber: 1, Offset: 100}
		nonKeyColumns := []byte{0xAA, 0xBB, 0xCC}

		prefix := encodeRecordNonKeyPrefix(lastModified, rollPtr)
		nonKeyBytes := append(prefix, nonKeyColumns...)

		// WHEN
		decodedLastModified, decodedRollPtr, decodedNonKeyColumns := decodeRecordNonKey(nonKeyBytes)

		// THEN
		assert.Equal(t, lastModified, decodedLastModified)
		assert.Equal(t, rollPtr, decodedRollPtr)
		assert.Equal(t, nonKeyColumns, decodedNonKeyColumns)
	})

	t.Run("非キーカラムが空でもデコードできる", func(t *testing.T) {
		// GIVEN
		lastModified := TrxId(1)
		rollPtr := NullUndoPtr
		nonKeyBytes := encodeRecordNonKeyPrefix(lastModified, rollPtr)

		// WHEN
		decodedLastModified, decodedRollPtr, decodedNonKeyColumns := decodeRecordNonKey(nonKeyBytes)

		// THEN
		assert.Equal(t, lastModified, decodedLastModified)
		assert.Equal(t, rollPtr, decodedRollPtr)
		assert.Equal(t, 0, len(decodedNonKeyColumns))
	})
}
