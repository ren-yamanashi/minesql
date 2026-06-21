package undo

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestDDLRecordTypeValues(t *testing.T) {
	t.Run("各 DDL Undo タイプの値が互いに衝突せず既存 RecordType と独立した型である", func(t *testing.T) {
		// GIVEN
		types := []DDLRecordType{
			DDLRecordTypeCreateBTree,
			DDLRecordTypeMetaInsert,
			DDLRecordTypeAllocateFileId,
		}

		// WHEN
		seen := make(map[DDLRecordType]struct{}, len(types))
		for _, tt := range types {
			seen[tt] = struct{}{}
		}

		// THEN
		assert.Len(t, seen, len(types))
		for _, tt := range types {
			assert.NotEqual(t, ddlRecordTypeUnknown, tt)
		}
	})
}

func TestDDLRecordTypeString(t *testing.T) {
	t.Run("既知の DDL Undo タイプはタイプ名に対応する文字列を返す", func(t *testing.T) {
		// GIVEN
		cases := map[DDLRecordType]string{
			DDLRecordTypeCreateBTree:    "CreateBTree",
			DDLRecordTypeMetaInsert:     "MetaInsert",
			DDLRecordTypeAllocateFileId: "AllocateFileId",
		}

		for tt, expected := range cases {
			// WHEN
			actual := tt.String()

			// THEN
			assert.Equal(t, expected, actual)
		}
	})

	t.Run("未知の DDL Undo タイプは Unknown を返す", func(t *testing.T) {
		// GIVEN
		unknown := ddlRecordTypeUnknown

		// WHEN
		actual := unknown.String()

		// THEN
		assert.Equal(t, "Unknown", actual)
	})
}

func TestNewDDLRecord(t *testing.T) {
	t.Run("RecordType と Payload を保持する", func(t *testing.T) {
		// GIVEN
		payload := []byte{0x10, 0x20, 0x30}

		// WHEN
		record := NewDDLRecord(DDLRecordTypeCreateBTree, payload)

		// THEN
		assert.Equal(t, DDLRecordTypeCreateBTree, record.RecordType())
		assert.Equal(t, payload, record.Payload())
	})
}

func TestDDLRecordPayload(t *testing.T) {
	t.Run("呼び出すたびに独立した copy が返る", func(t *testing.T) {
		// GIVEN
		record := NewDDLRecord(DDLRecordTypeCreateBTree, []byte{0x10, 0x20, 0x30})

		// WHEN
		first := record.Payload()
		first[0] = 0xFF
		second := record.Payload()

		// THEN
		assert.Equal(t, []byte{0x10, 0x20, 0x30}, second)
	})
}

func TestDDLRecordSerialize(t *testing.T) {
	t.Run("Header (3B) と payload を順に連結する", func(t *testing.T) {
		// GIVEN
		record := NewDDLRecord(DDLRecordTypeMetaInsert, []byte{0xAA, 0xBB})

		// WHEN
		buf := record.Serialize()

		// THEN
		assert.Len(t, buf, ddlRecordHeaderSize+2)
		assert.Equal(t, byte(DDLRecordTypeMetaInsert), buf[0])
		assert.Equal(t, byte(0x00), buf[1])
		assert.Equal(t, byte(0x02), buf[2])
		assert.Equal(t, []byte{0xAA, 0xBB}, buf[3:])
	})

	t.Run("payload が空でも header だけのバイト列を返す", func(t *testing.T) {
		// GIVEN
		record := NewDDLRecord(DDLRecordTypeAllocateFileId, nil)

		// WHEN
		buf := record.Serialize()

		// THEN
		assert.Len(t, buf, ddlRecordHeaderSize)
		assert.Equal(t, byte(DDLRecordTypeAllocateFileId), buf[0])
		assert.Equal(t, byte(0x00), buf[1])
		assert.Equal(t, byte(0x00), buf[2])
	})
}

func TestDeserializeDDLRecord(t *testing.T) {
	t.Run("Serialize の出力を読み戻せる", func(t *testing.T) {
		// GIVEN
		original := NewDDLRecord(DDLRecordTypeCreateBTree, []byte("hello"))
		buf := original.Serialize()

		// WHEN
		got, consumed, err := DeserializeDDLRecord(buf)

		// THEN
		assert.NoError(t, err)
		assert.Equal(t, len(buf), consumed)
		assert.Equal(t, original.RecordType(), got.RecordType())
		assert.Equal(t, original.Payload(), got.Payload())
	})

	t.Run("複数レコードを連結したバッファの先頭から 1 件分だけ読み取る", func(t *testing.T) {
		// GIVEN
		r1 := NewDDLRecord(DDLRecordTypeCreateBTree, []byte("aa"))
		r2 := NewDDLRecord(DDLRecordTypeMetaInsert, []byte("bbb"))
		buf := append(r1.Serialize(), r2.Serialize()...)

		// WHEN
		got, consumed, err := DeserializeDDLRecord(buf)

		// THEN
		assert.NoError(t, err)
		assert.Equal(t, len(r1.Serialize()), consumed)
		assert.Equal(t, r1.RecordType(), got.RecordType())
		assert.Equal(t, r1.Payload(), got.Payload())
	})

	t.Run("バッファが header サイズ未満の場合 ErrInvalidDDLRecord", func(t *testing.T) {
		// GIVEN
		buf := []byte{0x01}

		// WHEN
		_, _, err := DeserializeDDLRecord(buf)

		// THEN
		assert.ErrorIs(t, err, ErrInvalidDDLRecord)
	})

	t.Run("Header が示す payload 長に満たない場合 ErrInvalidDDLRecord", func(t *testing.T) {
		// GIVEN
		buf := []byte{byte(DDLRecordTypeCreateBTree), 0x00, 0x05, 0xAA, 0xBB}

		// WHEN
		_, _, err := DeserializeDDLRecord(buf)

		// THEN
		assert.ErrorIs(t, err, ErrInvalidDDLRecord)
	})
}
