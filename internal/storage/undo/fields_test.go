package undo

import (
	"encoding/binary"
	"testing"

	"github.com/ren-yamanashi/minesql/internal/storage/lock"
	"github.com/ren-yamanashi/minesql/internal/storage/page"
	"github.com/stretchr/testify/assert"
)

func TestSerialize(t *testing.T) {
	t.Run("ヘッダーにフィールド値が正しくエンコードされる", func(t *testing.T) {
		// GIVEN
		f := &Fields{
			TrxId:         10,
			UndoNum:       3,
			RecordType:    RecordTypeInsert,
			PrevLastTrxId: 100,
			PrevRollPtr:   newPointer(5, 128),
			TableFileId:   page.FileId(7),
			ColumnSets:    [][][]byte{{[]byte("a")}},
		}

		// WHEN
		buf := f.Serialize()

		// THEN
		assert.Equal(t, uint32(10), binary.BigEndian.Uint32(buf[headerTrxIdOffset:headerUndoNumOffset]))
		assert.Equal(t, uint32(3), binary.BigEndian.Uint32(buf[headerUndoNumOffset:headerRecordTypeOffset]))
		assert.Equal(t, byte(RecordTypeInsert), buf[headerRecordTypeOffset])
		dataLen := binary.BigEndian.Uint16(buf[headerDataLenOffset:recordHeaderSize])
		assert.Equal(t, len(buf)-recordHeaderSize, int(dataLen))
	})

	t.Run("1 カラムセットでシリアライズできる", func(t *testing.T) {
		// GIVEN
		f := &Fields{
			TrxId:         1,
			UndoNum:       2,
			RecordType:    RecordTypeInsert,
			PrevLastTrxId: 100,
			PrevRollPtr:   newPointer(3, 64),
			TableFileId:   page.FileId(5),
			ColumnSets:    [][][]byte{{[]byte("alice"), []byte("bob")}},
		}

		// WHEN
		buf := f.Serialize()

		// THEN
		assert.NotEmpty(t, buf)
		assert.True(t, len(buf) > recordHeaderSize)
	})

	t.Run("空のカラムセットでシリアライズできる", func(t *testing.T) {
		// GIVEN
		f := &Fields{
			TrxId:         1,
			UndoNum:       0,
			RecordType:    RecordTypeDelete,
			PrevLastTrxId: 0,
			PrevRollPtr:   NullPointer,
			TableFileId:   page.FileId(1),
			ColumnSets:    [][][]byte{{}},
		}

		// WHEN
		buf := f.Serialize()

		// THEN
		assert.NotEmpty(t, buf)
	})

	t.Run("カラムセットなしでシリアライズできる", func(t *testing.T) {
		// GIVEN
		f := &Fields{
			TrxId:         1,
			UndoNum:       0,
			RecordType:    RecordTypeInsert,
			PrevLastTrxId: 0,
			PrevRollPtr:   NullPointer,
			TableFileId:   page.FileId(1),
			ColumnSets:    [][][]byte{},
		}

		// WHEN
		buf := f.Serialize()

		// THEN
		// ヘッダー (11B) + prevLastTrxId (4B) + prevRollPtr (4B) + tableFileId (4B) = 23B
		assert.Equal(t, recordHeaderSize+lock.TrxIdSize+PointerSize+page.FileIdSize, len(buf))
	})
}

func TestDeserializeFields(t *testing.T) {
	t.Run("Serialize した結果を Deserialize でラウンドトリップできる", func(t *testing.T) {
		// GIVEN
		original := &Fields{
			TrxId:         10,
			UndoNum:       3,
			RecordType:    RecordTypeInsert,
			PrevLastTrxId: 200,
			PrevRollPtr:   newPointer(5, 128),
			TableFileId:   page.FileId(7),
			ColumnSets:    [][][]byte{{[]byte("alice"), []byte("bob")}},
		}
		buf := original.Serialize()

		// WHEN
		restored, err := DeserializeFields(buf)

		// THEN
		assert.NoError(t, err)
		assertFieldsEqual(t, *original, restored)
	})

	t.Run("2 カラムセットでラウンドトリップできる", func(t *testing.T) {
		// GIVEN
		original := &Fields{
			TrxId:         5,
			UndoNum:       1,
			RecordType:    RecordTypeUpdate,
			PrevLastTrxId: 50,
			PrevRollPtr:   newPointer(2, 32),
			TableFileId:   page.FileId(3),
			ColumnSets: [][][]byte{
				{[]byte("old_val1"), []byte("old_val2")},
				{[]byte("new_val1"), []byte("new_val2")},
			},
		}
		buf := original.Serialize()

		// WHEN
		restored, err := DeserializeFields(buf)

		// THEN
		assert.NoError(t, err)
		assertFieldsEqual(t, *original, restored)
	})

	t.Run("NullPointer でラウンドトリップできる", func(t *testing.T) {
		// GIVEN
		original := &Fields{
			TrxId:         1,
			UndoNum:       0,
			RecordType:    RecordTypeDelete,
			PrevLastTrxId: 0,
			PrevRollPtr:   NullPointer,
			TableFileId:   page.FileId(1),
			ColumnSets:    [][][]byte{{[]byte("data")}},
		}
		buf := original.Serialize()

		// WHEN
		restored, err := DeserializeFields(buf)

		// THEN
		assert.NoError(t, err)
		assert.Equal(t, NullPointer, restored.PrevRollPtr)
	})

	t.Run("大きい TrxId でラウンドトリップできる", func(t *testing.T) {
		// GIVEN
		original := &Fields{
			TrxId:         lock.TrxId(0xFFFFFFFF),
			UndoNum:       UndoNumber(0xFFFFFFFE),
			RecordType:    RecordTypeInsert,
			PrevLastTrxId: lock.TrxId(0xFFFFFFFD),
			PrevRollPtr:   newPointer(1, 10),
			TableFileId:   page.FileId(1),
			ColumnSets:    [][][]byte{{[]byte("x")}},
		}
		buf := original.Serialize()

		// WHEN
		restored, err := DeserializeFields(buf)

		// THEN
		assert.NoError(t, err)
		assert.Equal(t, original.TrxId, restored.TrxId)
		assert.Equal(t, original.UndoNum, restored.UndoNum)
		assert.Equal(t, original.PrevLastTrxId, restored.PrevLastTrxId)
	})

	t.Run("カラムセットなしでラウンドトリップできる", func(t *testing.T) {
		// GIVEN
		original := &Fields{
			TrxId:         1,
			UndoNum:       0,
			RecordType:    RecordTypeInsert,
			PrevLastTrxId: 0,
			PrevRollPtr:   NullPointer,
			TableFileId:   page.FileId(1),
			ColumnSets:    [][][]byte{},
		}
		buf := original.Serialize()

		// WHEN
		restored, err := DeserializeFields(buf)

		// THEN
		assert.NoError(t, err)
		assert.Empty(t, restored.ColumnSets)
	})

	t.Run("空のカラムデータでラウンドトリップできる", func(t *testing.T) {
		// GIVEN
		original := &Fields{
			TrxId:         1,
			UndoNum:       0,
			RecordType:    RecordTypeInsert,
			PrevLastTrxId: 0,
			PrevRollPtr:   NullPointer,
			TableFileId:   page.FileId(1),
			ColumnSets:    [][][]byte{{[]byte{}, []byte("data"), []byte{}}},
		}
		buf := original.Serialize()

		// WHEN
		restored, err := DeserializeFields(buf)

		// THEN
		assert.NoError(t, err)
		assert.Equal(t, original.ColumnSets, restored.ColumnSets)
	})

	t.Run("全 RecordType でラウンドトリップできる", func(t *testing.T) {
		// GIVEN
		recordTypes := []RecordType{RecordTypeInsert, RecordTypeDelete, RecordTypeUpdate}
		for _, rt := range recordTypes {
			original := &Fields{
				TrxId:         1,
				UndoNum:       0,
				RecordType:    rt,
				PrevLastTrxId: 0,
				PrevRollPtr:   NullPointer,
				TableFileId:   page.FileId(1),
				ColumnSets:    [][][]byte{{[]byte("data")}},
			}
			buf := original.Serialize()

			// WHEN
			restored, err := DeserializeFields(buf)

			// THEN
			assert.NoError(t, err)
			assert.Equal(t, rt, restored.RecordType)
		}
	})

	t.Run("バッファが空の場合エラーを返す", func(t *testing.T) {
		// GIVEN
		buf := []byte{}

		// WHEN
		_, err := DeserializeFields(buf)

		// THEN
		assert.ErrorIs(t, err, ErrInvalidRecord)
	})

	t.Run("バッファが headerSize 未満の場合エラーを返す", func(t *testing.T) {
		// GIVEN
		buf := make([]byte, recordHeaderSize-1)

		// WHEN
		_, err := DeserializeFields(buf)

		// THEN
		assert.ErrorIs(t, err, ErrInvalidRecord)
	})

	t.Run("DataLen がバッファサイズを超える場合エラーを返す", func(t *testing.T) {
		// GIVEN
		f := &Fields{
			TrxId:       1,
			UndoNum:     0,
			RecordType:  RecordTypeInsert,
			PrevRollPtr: NullPointer,
			TableFileId: page.FileId(1),
			ColumnSets:  [][][]byte{{[]byte("data")}},
		}
		buf := f.Serialize()
		truncated := buf[:recordHeaderSize+2]

		// WHEN
		_, err := DeserializeFields(truncated)

		// THEN
		assert.ErrorIs(t, err, ErrInvalidRecord)
	})

	t.Run("データ部が prevFields に満たない場合エラーを返す", func(t *testing.T) {
		// GIVEN: prevLastTrxId (4B) + prevRollPtr (4B) = 8B 必要だが 4B しかない
		data := make([]byte, 4)
		buf := buildRawBuffer(1, 0, RecordTypeInsert, data)

		// WHEN
		_, err := DeserializeFields(buf)

		// THEN
		assert.ErrorIs(t, err, ErrInvalidRecord)
	})

	t.Run("データ部が FileId に満たない場合エラーを返す", func(t *testing.T) {
		// GIVEN: prevLastTrxId + prevRollPtr (8B) は足りるが FileId (4B) が不足
		var data []byte
		data = binary.BigEndian.AppendUint32(data, 100)
		data = append(data, NullPointer.Encode()...)
		buf := buildRawBuffer(1, 0, RecordTypeInsert, data)

		// WHEN
		_, err := DeserializeFields(buf)

		// THEN
		assert.ErrorIs(t, err, ErrInvalidRecord)
	})

	t.Run("カラムセット領域が columnCountSize 未満の場合エラーを返す", func(t *testing.T) {
		// GIVEN: 固定フィールドは正常だがカラムセット領域が 1 バイトしかない
		var data []byte
		data = binary.BigEndian.AppendUint32(data, 100)       // prevLastTrxId
		data = append(data, NullPointer.Encode()...)          // prevRollPtr
		data = binary.BigEndian.AppendUint32(data, uint32(1)) // tableFileId
		data = append(data, 0x01)                             // 1 バイト (columnCountSize 未満)
		buf := buildRawBuffer(1, 0, RecordTypeInsert, data)

		// WHEN
		_, err := DeserializeFields(buf)

		// THEN
		assert.ErrorIs(t, err, ErrInvalidRecord)
	})

	t.Run("カラムデータ長ヘッダーが不足する場合エラーを返す", func(t *testing.T) {
		// GIVEN: numCols = 2 だがカラムデータが 1 つもない
		var data []byte
		data = binary.BigEndian.AppendUint32(data, 100)       // prevLastTrxId
		data = append(data, NullPointer.Encode()...)          // prevRollPtr
		data = binary.BigEndian.AppendUint32(data, uint32(1)) // tableFileId
		data = binary.BigEndian.AppendUint16(data, 2)         // numCols = 2
		buf := buildRawBuffer(1, 0, RecordTypeInsert, data)

		// WHEN
		_, err := DeserializeFields(buf)

		// THEN
		assert.ErrorIs(t, err, ErrInvalidRecord)
	})

	t.Run("カラムデータ本体が不足する場合エラーを返す", func(t *testing.T) {
		// GIVEN: numCols = 1, colLen = 100 だが実データがない
		var data []byte
		data = binary.BigEndian.AppendUint32(data, 100)       // prevLastTrxId
		data = append(data, NullPointer.Encode()...)          // prevRollPtr
		data = binary.BigEndian.AppendUint32(data, uint32(1)) // tableFileId
		data = binary.BigEndian.AppendUint16(data, 1)         // numCols = 1
		data = binary.BigEndian.AppendUint16(data, 100)       // colLen = 100
		buf := buildRawBuffer(1, 0, RecordTypeInsert, data)

		// WHEN
		_, err := DeserializeFields(buf)

		// THEN
		assert.ErrorIs(t, err, ErrInvalidRecord)
	})
}

// buildRawBuffer はテスト用にヘッダーと任意のデータ部から Undo レコードのバイト列を構築する
func buildRawBuffer(trxId lock.TrxId, undoNum UndoNumber, recordType RecordType, data []byte) []byte {
	buf := make([]byte, recordHeaderSize+len(data))
	binary.BigEndian.PutUint32(buf[headerTrxIdOffset:headerUndoNumOffset], trxId)
	binary.BigEndian.PutUint32(buf[headerUndoNumOffset:headerRecordTypeOffset], undoNum)
	buf[headerRecordTypeOffset] = byte(recordType)
	binary.BigEndian.PutUint16(buf[headerDataLenOffset:recordHeaderSize], uint16(len(data)))
	copy(buf[recordHeaderSize:], data)
	return buf
}

// assertFieldsEqual は 2 つの Fields の全フィールドが等しいことを検証する
func assertFieldsEqual(t *testing.T, expected, actual Fields) {
	t.Helper()
	assert.Equal(t, expected.TrxId, actual.TrxId)
	assert.Equal(t, expected.UndoNum, actual.UndoNum)
	assert.Equal(t, expected.RecordType, actual.RecordType)
	assert.Equal(t, expected.PrevLastTrxId, actual.PrevLastTrxId)
	assert.Equal(t, expected.PrevRollPtr, actual.PrevRollPtr)
	assert.Equal(t, expected.TableFileId, actual.TableFileId)
	assert.Equal(t, expected.ColumnSets, actual.ColumnSets)
}
