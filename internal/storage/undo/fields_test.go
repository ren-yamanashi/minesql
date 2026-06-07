package undo

import (
	"encoding/binary"
	"testing"

	"github.com/ren-yamanashi/minesql/internal/storage/lock"
	"github.com/ren-yamanashi/minesql/internal/storage/page"
	"github.com/stretchr/testify/assert"
)

func TestFieldsTrxId(t *testing.T) {
	t.Run("設定した TrxId を返す", func(t *testing.T) {
		// GIVEN
		f := &Fields{trxId: 42}

		// WHEN
		result := f.TrxId()

		// THEN
		assert.Equal(t, lock.TrxId(42), result)
	})
}

func TestFieldsToRecord(t *testing.T) {
	t.Run("Insert の Fields を InsertRecord に変換できる", func(t *testing.T) {
		// GIVEN
		f := &Fields{
			trxId:         1,
			undoNumber:    0,
			recordType:    RecordTypeInsert,
			prevLastTrxId: 0,
			prevRollPtr:   NullPointer(),
			tableFileId:   page.FileId(5),
			columnSets:    [][][]byte{{[]byte("alice"), []byte("bob")}},
		}

		// WHEN
		record, err := f.ToRecord()

		// THEN
		assert.NoError(t, err)
		ir, ok := record.(InsertRecord)
		assert.True(t, ok)
		assert.Equal(t, page.FileId(5), ir.TableFileId())
		assert.Equal(t, [][]byte{[]byte("alice"), []byte("bob")}, [][]byte(ir.record))
	})

	t.Run("Delete の Fields を DeleteRecord に変換できる", func(t *testing.T) {
		// GIVEN
		f := &Fields{
			trxId:         2,
			undoNumber:    1,
			recordType:    RecordTypeDelete,
			prevLastTrxId: 100,
			prevRollPtr:   NewPointer(3, 64),
			tableFileId:   page.FileId(7),
			columnSets:    [][][]byte{{[]byte("data")}},
		}

		// WHEN
		record, err := f.ToRecord()

		// THEN
		assert.NoError(t, err)
		dr, ok := record.(DeleteRecord)
		assert.True(t, ok)
		assert.Equal(t, page.FileId(7), dr.TableFileId())
		assert.Equal(t, lock.TrxId(100), dr.prevLastTrxId)
	})

	t.Run("Update の Fields を UpdateRecord に変換できる", func(t *testing.T) {
		// GIVEN
		f := &Fields{
			trxId:         3,
			undoNumber:    2,
			recordType:    RecordTypeUpdate,
			prevLastTrxId: 50,
			prevRollPtr:   NewPointer(2, 32),
			tableFileId:   page.FileId(9),
			columnSets: [][][]byte{
				{[]byte("old_val")},
				{[]byte("new_val")},
			},
		}

		// WHEN
		record, err := f.ToRecord()

		// THEN
		assert.NoError(t, err)
		ur, ok := record.(UpdateRecord)
		assert.True(t, ok)
		assert.Equal(t, page.FileId(9), ur.TableFileId())
		assert.Equal(t, [][]byte{[]byte("old_val")}, [][]byte(ur.prevRecord))
		assert.Equal(t, [][]byte{[]byte("new_val")}, [][]byte(ur.newRecord))
	})

	t.Run("Insert で ColumnSets が空の場合エラーを返す", func(t *testing.T) {
		// GIVEN
		f := &Fields{
			recordType: RecordTypeInsert,
			columnSets: [][][]byte{},
		}

		// WHEN
		_, err := f.ToRecord()

		// THEN
		assert.ErrorIs(t, err, ErrInvalidRecord)
	})

	t.Run("Delete で ColumnSets が空の場合エラーを返す", func(t *testing.T) {
		// GIVEN
		f := &Fields{
			recordType: RecordTypeDelete,
			columnSets: [][][]byte{},
		}

		// WHEN
		_, err := f.ToRecord()

		// THEN
		assert.ErrorIs(t, err, ErrInvalidRecord)
	})

	t.Run("Update で ColumnSets が 1 つしかない場合エラーを返す", func(t *testing.T) {
		// GIVEN
		f := &Fields{
			recordType: RecordTypeUpdate,
			columnSets: [][][]byte{{[]byte("only_one")}},
		}

		// WHEN
		_, err := f.ToRecord()

		// THEN
		assert.ErrorIs(t, err, ErrInvalidRecord)
	})

	t.Run("Update で ColumnSets が空の場合エラーを返す", func(t *testing.T) {
		// GIVEN
		f := &Fields{
			recordType: RecordTypeUpdate,
			columnSets: [][][]byte{},
		}

		// WHEN
		_, err := f.ToRecord()

		// THEN
		assert.ErrorIs(t, err, ErrInvalidRecord)
	})

	t.Run("不明な RecordType の場合エラーを返す", func(t *testing.T) {
		// GIVEN
		f := &Fields{
			recordType: RecordType(99),
			columnSets: [][][]byte{{[]byte("data")}},
		}

		// WHEN
		_, err := f.ToRecord()

		// THEN
		assert.ErrorIs(t, err, ErrInvalidRecord)
	})
}

func TestFieldsSerialize(t *testing.T) {
	t.Run("ヘッダーにフィールド値が正しくエンコードされる", func(t *testing.T) {
		// GIVEN
		f := &Fields{
			trxId:         10,
			undoNumber:    3,
			recordType:    RecordTypeInsert,
			prevLastTrxId: 100,
			prevRollPtr:   NewPointer(5, 128),
			tableFileId:   page.FileId(7),
			columnSets:    [][][]byte{{[]byte("a")}},
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
			trxId:         1,
			undoNumber:    2,
			recordType:    RecordTypeInsert,
			prevLastTrxId: 100,
			prevRollPtr:   NewPointer(3, 64),
			tableFileId:   page.FileId(5),
			columnSets:    [][][]byte{{[]byte("alice"), []byte("bob")}},
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
			trxId:         1,
			undoNumber:    0,
			recordType:    RecordTypeDelete,
			prevLastTrxId: 0,
			prevRollPtr:   NullPointer(),
			tableFileId:   page.FileId(1),
			columnSets:    [][][]byte{{}},
		}

		// WHEN
		buf := f.Serialize()

		// THEN
		assert.NotEmpty(t, buf)
	})

	t.Run("カラムセットなしでシリアライズできる", func(t *testing.T) {
		// GIVEN
		f := &Fields{
			trxId:         1,
			undoNumber:    0,
			recordType:    RecordTypeInsert,
			prevLastTrxId: 0,
			prevRollPtr:   NullPointer(),
			tableFileId:   page.FileId(1),
			columnSets:    [][][]byte{},
		}

		// WHEN
		buf := f.Serialize()

		// THEN
		assert.Equal(t, recordHeaderSize+lock.TrxIdSize+PointerSize+page.FileIdSize, len(buf))
	})
}

func TestDeserializeFields(t *testing.T) {
	t.Run("Serialize した結果を Deserialize でラウンドトリップできる", func(t *testing.T) {
		// GIVEN
		original := &Fields{
			trxId:         10,
			undoNumber:    3,
			recordType:    RecordTypeInsert,
			prevLastTrxId: 200,
			prevRollPtr:   NewPointer(5, 128),
			tableFileId:   page.FileId(7),
			columnSets:    [][][]byte{{[]byte("alice"), []byte("bob")}},
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
			trxId:         5,
			undoNumber:    1,
			recordType:    RecordTypeUpdate,
			prevLastTrxId: 50,
			prevRollPtr:   NewPointer(2, 32),
			tableFileId:   page.FileId(3),
			columnSets: [][][]byte{
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
			trxId:         1,
			undoNumber:    0,
			recordType:    RecordTypeDelete,
			prevLastTrxId: 0,
			prevRollPtr:   NullPointer(),
			tableFileId:   page.FileId(1),
			columnSets:    [][][]byte{{[]byte("data")}},
		}
		buf := original.Serialize()

		// WHEN
		restored, err := DeserializeFields(buf)

		// THEN
		assert.NoError(t, err)
		assert.Equal(t, NullPointer(), restored.prevRollPtr)
	})

	t.Run("大きい TrxId でラウンドトリップできる", func(t *testing.T) {
		// GIVEN
		original := &Fields{
			trxId:         lock.TrxId(0xFFFFFFFF),
			undoNumber:    UndoNumber(0xFFFFFFFE),
			recordType:    RecordTypeInsert,
			prevLastTrxId: lock.TrxId(0xFFFFFFFD),
			prevRollPtr:   NewPointer(1, 10),
			tableFileId:   page.FileId(1),
			columnSets:    [][][]byte{{[]byte("x")}},
		}
		buf := original.Serialize()

		// WHEN
		restored, err := DeserializeFields(buf)

		// THEN
		assert.NoError(t, err)
		assert.Equal(t, original.trxId, restored.trxId)
		assert.Equal(t, original.undoNumber, restored.undoNumber)
		assert.Equal(t, original.prevLastTrxId, restored.prevLastTrxId)
	})

	t.Run("カラムセットなしでラウンドトリップできる", func(t *testing.T) {
		// GIVEN
		original := &Fields{
			trxId:         1,
			undoNumber:    0,
			recordType:    RecordTypeInsert,
			prevLastTrxId: 0,
			prevRollPtr:   NullPointer(),
			tableFileId:   page.FileId(1),
			columnSets:    [][][]byte{},
		}
		buf := original.Serialize()

		// WHEN
		restored, err := DeserializeFields(buf)

		// THEN
		assert.NoError(t, err)
		assert.Empty(t, restored.columnSets)
	})

	t.Run("空のカラムデータでラウンドトリップできる", func(t *testing.T) {
		// GIVEN
		original := &Fields{
			trxId:         1,
			undoNumber:    0,
			recordType:    RecordTypeInsert,
			prevLastTrxId: 0,
			prevRollPtr:   NullPointer(),
			tableFileId:   page.FileId(1),
			columnSets:    [][][]byte{{[]byte{}, []byte("data"), []byte{}}},
		}
		buf := original.Serialize()

		// WHEN
		restored, err := DeserializeFields(buf)

		// THEN
		assert.NoError(t, err)
		assert.Equal(t, original.columnSets, restored.columnSets)
	})

	t.Run("全 RecordType でラウンドトリップできる", func(t *testing.T) {
		// GIVEN
		recordTypes := []RecordType{RecordTypeInsert, RecordTypeDelete, RecordTypeUpdate}
		for _, rt := range recordTypes {
			original := &Fields{
				trxId:         1,
				undoNumber:    0,
				recordType:    rt,
				prevLastTrxId: 0,
				prevRollPtr:   NullPointer(),
				tableFileId:   page.FileId(1),
				columnSets:    [][][]byte{{[]byte("data")}},
			}
			buf := original.Serialize()

			// WHEN
			restored, err := DeserializeFields(buf)

			// THEN
			assert.NoError(t, err)
			assert.Equal(t, rt, restored.recordType)
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
			trxId:       1,
			undoNumber:  0,
			recordType:  RecordTypeInsert,
			prevRollPtr: NullPointer(),
			tableFileId: page.FileId(1),
			columnSets:  [][][]byte{{[]byte("data")}},
		}
		buf := f.Serialize()
		truncated := buf[:recordHeaderSize+2]

		// WHEN
		_, err := DeserializeFields(truncated)

		// THEN
		assert.ErrorIs(t, err, ErrInvalidRecord)
	})

	t.Run("データ部が prevFields に満たない場合エラーを返す", func(t *testing.T) {
		// GIVEN
		data := make([]byte, 4)
		buf := buildRawBuffer(1, 0, RecordTypeInsert, data)

		// WHEN
		_, err := DeserializeFields(buf)

		// THEN
		assert.ErrorIs(t, err, ErrInvalidRecord)
	})

	t.Run("データ部が FileId に満たない場合エラーを返す", func(t *testing.T) {
		// GIVEN
		var data []byte
		data = binary.BigEndian.AppendUint32(data, 100)
		data = append(data, NullPointer().Encode()...)
		buf := buildRawBuffer(1, 0, RecordTypeInsert, data)

		// WHEN
		_, err := DeserializeFields(buf)

		// THEN
		assert.ErrorIs(t, err, ErrInvalidRecord)
	})

	t.Run("カラムセット領域が columnCountSize 未満の場合エラーを返す", func(t *testing.T) {
		// GIVEN
		var data []byte
		data = binary.BigEndian.AppendUint32(data, 100)
		data = append(data, NullPointer().Encode()...)
		data = binary.BigEndian.AppendUint32(data, uint32(1))
		data = append(data, 0x01)
		buf := buildRawBuffer(1, 0, RecordTypeInsert, data)

		// WHEN
		_, err := DeserializeFields(buf)

		// THEN
		assert.ErrorIs(t, err, ErrInvalidRecord)
	})

	t.Run("カラムデータ長ヘッダーが不足する場合エラーを返す", func(t *testing.T) {
		// GIVEN
		var data []byte
		data = binary.BigEndian.AppendUint32(data, 100)
		data = append(data, NullPointer().Encode()...)
		data = binary.BigEndian.AppendUint32(data, uint32(1))
		data = binary.BigEndian.AppendUint16(data, 2)
		buf := buildRawBuffer(1, 0, RecordTypeInsert, data)

		// WHEN
		_, err := DeserializeFields(buf)

		// THEN
		assert.ErrorIs(t, err, ErrInvalidRecord)
	})

	t.Run("カラムデータ本体が不足する場合エラーを返す", func(t *testing.T) {
		// GIVEN
		var data []byte
		data = binary.BigEndian.AppendUint32(data, 100)
		data = append(data, NullPointer().Encode()...)
		data = binary.BigEndian.AppendUint32(data, uint32(1))
		data = binary.BigEndian.AppendUint16(data, 1)
		data = binary.BigEndian.AppendUint16(data, 100)
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
	binary.BigEndian.PutUint32(buf[headerTrxIdOffset:headerUndoNumOffset], uint32(trxId))
	binary.BigEndian.PutUint32(buf[headerUndoNumOffset:headerRecordTypeOffset], undoNum)
	buf[headerRecordTypeOffset] = byte(recordType)
	binary.BigEndian.PutUint16(buf[headerDataLenOffset:recordHeaderSize], uint16(len(data)))
	copy(buf[recordHeaderSize:], data)
	return buf
}

// assertFieldsEqual は 2 つの Fields の全フィールドが等しいことを検証する
func assertFieldsEqual(t *testing.T, expected, actual Fields) {
	t.Helper()
	assert.Equal(t, expected.trxId, actual.trxId)
	assert.Equal(t, expected.undoNumber, actual.undoNumber)
	assert.Equal(t, expected.recordType, actual.recordType)
	assert.Equal(t, expected.prevLastTrxId, actual.prevLastTrxId)
	assert.Equal(t, expected.prevRollPtr, actual.prevRollPtr)
	assert.Equal(t, expected.tableFileId, actual.tableFileId)
	assert.Equal(t, expected.columnSets, actual.columnSets)
}
