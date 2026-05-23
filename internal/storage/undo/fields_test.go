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

func TestFieldsUndoNum(t *testing.T) {
	t.Run("設定した UndoNum を返す", func(t *testing.T) {
		// GIVEN
		f := &Fields{undoNum: 7}

		// WHEN
		result := f.undoNum

		// THEN
		assert.Equal(t, UndoNumber(7), result)
	})
}

func TestFieldsRecordType(t *testing.T) {
	t.Run("設定した RecordType を返す", func(t *testing.T) {
		// GIVEN
		f := &Fields{recordType: RecordTypeUpdate}

		// WHEN
		result := f.recordType

		// THEN
		assert.Equal(t, RecordTypeUpdate, result)
	})
}

func TestFieldsPrevLastTrxId(t *testing.T) {
	t.Run("設定した PrevLastTrxId を返す", func(t *testing.T) {
		// GIVEN
		f := &Fields{prevLastTrxId: 100}

		// WHEN
		result := f.prevLastTrxId

		// THEN
		assert.Equal(t, lock.TrxId(100), result)
	})
}

func TestFieldsPrevRollPtr(t *testing.T) {
	t.Run("設定した PrevRollPtr を返す", func(t *testing.T) {
		// GIVEN
		ptr := NewPointer(5, 128)
		f := &Fields{prevRollPtr: ptr}

		// WHEN
		result := f.prevRollPtr

		// THEN
		assert.Equal(t, ptr, result)
	})

	t.Run("NullPointer を返す", func(t *testing.T) {
		// GIVEN
		f := &Fields{prevRollPtr: NullPointer}

		// WHEN
		result := f.prevRollPtr

		// THEN
		assert.Equal(t, NullPointer, result)
	})
}

func TestFieldsTableFileId(t *testing.T) {
	t.Run("設定した TableFileId を返す", func(t *testing.T) {
		// GIVEN
		f := &Fields{tableFileId: page.FileId(7)}

		// WHEN
		result := f.tableFileId

		// THEN
		assert.Equal(t, page.FileId(7), result)
	})
}

func TestFieldsColumnSets(t *testing.T) {
	t.Run("設定した ColumnSets を返す", func(t *testing.T) {
		// GIVEN
		cs := [][][]byte{{[]byte("a"), []byte("b")}, {[]byte("c")}}
		f := &Fields{columnSets: cs}

		// WHEN
		result := f.columnSets

		// THEN
		assert.Equal(t, cs, result)
	})

	t.Run("空の ColumnSets を返す", func(t *testing.T) {
		// GIVEN
		f := &Fields{columnSets: [][][]byte{}}

		// WHEN
		result := f.columnSets

		// THEN
		assert.Empty(t, result)
	})

	t.Run("nil の ColumnSets を返す", func(t *testing.T) {
		// GIVEN
		f := &Fields{}

		// WHEN
		result := f.columnSets

		// THEN
		assert.Nil(t, result)
	})
}

func TestFieldsSerialize(t *testing.T) {
	t.Run("ヘッダーにフィールド値が正しくエンコードされる", func(t *testing.T) {
		// GIVEN
		f := &Fields{
			trxId:         10,
			undoNum:       3,
			recordType:    RecordTypeInsert,
			prevLastTrxId: 100,
			prevRollPtr:   NewPointer(5, 128),
			tableFileId:   page.FileId(7),
			columnSets:    [][][]byte{{[]byte("a")}},
		}

		// WHEN
		buf := f.serialize()

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
			undoNum:       2,
			recordType:    RecordTypeInsert,
			prevLastTrxId: 100,
			prevRollPtr:   NewPointer(3, 64),
			tableFileId:   page.FileId(5),
			columnSets:    [][][]byte{{[]byte("alice"), []byte("bob")}},
		}

		// WHEN
		buf := f.serialize()

		// THEN
		assert.NotEmpty(t, buf)
		assert.True(t, len(buf) > recordHeaderSize)
	})

	t.Run("空のカラムセットでシリアライズできる", func(t *testing.T) {
		// GIVEN
		f := &Fields{
			trxId:         1,
			undoNum:       0,
			recordType:    RecordTypeDelete,
			prevLastTrxId: 0,
			prevRollPtr:   NullPointer,
			tableFileId:   page.FileId(1),
			columnSets:    [][][]byte{{}},
		}

		// WHEN
		buf := f.serialize()

		// THEN
		assert.NotEmpty(t, buf)
	})

	t.Run("カラムセットなしでシリアライズできる", func(t *testing.T) {
		// GIVEN
		f := &Fields{
			trxId:         1,
			undoNum:       0,
			recordType:    RecordTypeInsert,
			prevLastTrxId: 0,
			prevRollPtr:   NullPointer,
			tableFileId:   page.FileId(1),
			columnSets:    [][][]byte{},
		}

		// WHEN
		buf := f.serialize()

		// THEN
		// ヘッダー (11B) + prevLastTrxId (4B) + prevRollPtr (4B) + tableFileId (4B) = 23B
		assert.Equal(t, recordHeaderSize+lock.TrxIdSize+PointerSize+page.FileIdSize, len(buf))
	})
}

func TestDeserializeFields(t *testing.T) {
	t.Run("Serialize した結果を Deserialize でラウンドトリップできる", func(t *testing.T) {
		// GIVEN
		original := &Fields{
			trxId:         10,
			undoNum:       3,
			recordType:    RecordTypeInsert,
			prevLastTrxId: 200,
			prevRollPtr:   NewPointer(5, 128),
			tableFileId:   page.FileId(7),
			columnSets:    [][][]byte{{[]byte("alice"), []byte("bob")}},
		}
		buf := original.serialize()

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
			undoNum:       1,
			recordType:    RecordTypeUpdate,
			prevLastTrxId: 50,
			prevRollPtr:   NewPointer(2, 32),
			tableFileId:   page.FileId(3),
			columnSets: [][][]byte{
				{[]byte("old_val1"), []byte("old_val2")},
				{[]byte("new_val1"), []byte("new_val2")},
			},
		}
		buf := original.serialize()

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
			undoNum:       0,
			recordType:    RecordTypeDelete,
			prevLastTrxId: 0,
			prevRollPtr:   NullPointer,
			tableFileId:   page.FileId(1),
			columnSets:    [][][]byte{{[]byte("data")}},
		}
		buf := original.serialize()

		// WHEN
		restored, err := DeserializeFields(buf)

		// THEN
		assert.NoError(t, err)
		assert.Equal(t, NullPointer, restored.prevRollPtr)
	})

	t.Run("大きい TrxId でラウンドトリップできる", func(t *testing.T) {
		// GIVEN
		original := &Fields{
			trxId:         lock.TrxId(0xFFFFFFFF),
			undoNum:       UndoNumber(0xFFFFFFFE),
			recordType:    RecordTypeInsert,
			prevLastTrxId: lock.TrxId(0xFFFFFFFD),
			prevRollPtr:   NewPointer(1, 10),
			tableFileId:   page.FileId(1),
			columnSets:    [][][]byte{{[]byte("x")}},
		}
		buf := original.serialize()

		// WHEN
		restored, err := DeserializeFields(buf)

		// THEN
		assert.NoError(t, err)
		assert.Equal(t, original.trxId, restored.trxId)
		assert.Equal(t, original.undoNum, restored.undoNum)
		assert.Equal(t, original.prevLastTrxId, restored.prevLastTrxId)
	})

	t.Run("カラムセットなしでラウンドトリップできる", func(t *testing.T) {
		// GIVEN
		original := &Fields{
			trxId:         1,
			undoNum:       0,
			recordType:    RecordTypeInsert,
			prevLastTrxId: 0,
			prevRollPtr:   NullPointer,
			tableFileId:   page.FileId(1),
			columnSets:    [][][]byte{},
		}
		buf := original.serialize()

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
			undoNum:       0,
			recordType:    RecordTypeInsert,
			prevLastTrxId: 0,
			prevRollPtr:   NullPointer,
			tableFileId:   page.FileId(1),
			columnSets:    [][][]byte{{[]byte{}, []byte("data"), []byte{}}},
		}
		buf := original.serialize()

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
				undoNum:       0,
				recordType:    rt,
				prevLastTrxId: 0,
				prevRollPtr:   NullPointer,
				tableFileId:   page.FileId(1),
				columnSets:    [][][]byte{{[]byte("data")}},
			}
			buf := original.serialize()

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
			undoNum:     0,
			recordType:  RecordTypeInsert,
			prevRollPtr: NullPointer,
			tableFileId: page.FileId(1),
			columnSets:  [][][]byte{{[]byte("data")}},
		}
		buf := f.serialize()
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

func TestFieldsToRecord(t *testing.T) {
	t.Run("Insert の Fields を InsertRecord に変換できる", func(t *testing.T) {
		// GIVEN
		f := &Fields{
			trxId:         1,
			undoNum:       0,
			recordType:    RecordTypeInsert,
			prevLastTrxId: 0,
			prevRollPtr:   NullPointer,
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
			undoNum:       1,
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
			undoNum:       2,
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
	assert.Equal(t, expected.undoNum, actual.undoNum)
	assert.Equal(t, expected.recordType, actual.recordType)
	assert.Equal(t, expected.prevLastTrxId, actual.prevLastTrxId)
	assert.Equal(t, expected.prevRollPtr, actual.prevRollPtr)
	assert.Equal(t, expected.tableFileId, actual.tableFileId)
	assert.Equal(t, expected.columnSets, actual.columnSets)
}
