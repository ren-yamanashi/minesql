package undo

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestMetaTableTypeValues(t *testing.T) {
	t.Run("各 MetaTableType の値が互いに衝突せず番兵値と異なる", func(t *testing.T) {
		// GIVEN
		types := []MetaTableType{
			MetaTableTypeTable,
			MetaTableTypeIndex,
			MetaTableTypeIndexKeyColumn,
			MetaTableTypeColumn,
			MetaTableTypeConstraint,
		}

		// WHEN
		seen := make(map[MetaTableType]struct{}, len(types))
		for _, tt := range types {
			seen[tt] = struct{}{}
		}

		// THEN
		assert.Len(t, seen, len(types))
		for _, tt := range types {
			assert.NotEqual(t, metaTableTypeUnknown, tt)
		}
	})
}

func TestMetaTableTypeString(t *testing.T) {
	t.Run("既知の MetaTableType は対応する文字列を返す", func(t *testing.T) {
		// GIVEN
		cases := map[MetaTableType]string{
			MetaTableTypeTable:          "Table",
			MetaTableTypeIndex:          "Index",
			MetaTableTypeIndexKeyColumn: "IndexKeyColumn",
			MetaTableTypeColumn:         "Column",
			MetaTableTypeConstraint:     "Constraint",
		}

		for tt, expected := range cases {
			// WHEN
			actual := tt.String()

			// THEN
			assert.Equal(t, expected, actual)
		}
	})

	t.Run("未知の MetaTableType は Unknown を返す", func(t *testing.T) {
		// GIVEN
		unknown := metaTableTypeUnknown

		// WHEN
		actual := unknown.String()

		// THEN
		assert.Equal(t, "Unknown", actual)
	})
}

func TestNewMetaInsertUndoRecord(t *testing.T) {
	t.Run("MetaTableType と Key を保持する", func(t *testing.T) {
		// GIVEN
		key := []byte{0x01, 0x02, 0x03}

		// WHEN
		record := NewMetaInsertUndoRecord(MetaTableTypeTable, key)

		// THEN
		assert.Equal(t, MetaTableTypeTable, record.MetaTableType())
		assert.Equal(t, key, record.Key())
	})
}

func TestMetaInsertUndoRecordKey(t *testing.T) {
	t.Run("呼び出すたびに独立した copy が返る", func(t *testing.T) {
		// GIVEN
		record := NewMetaInsertUndoRecord(MetaTableTypeIndex, []byte{0xAA, 0xBB, 0xCC})

		// WHEN
		first := record.Key()
		first[0] = 0xFF
		second := record.Key()

		// THEN
		assert.Equal(t, []byte{0xAA, 0xBB, 0xCC}, second)
	})
}

func TestMetaInsertUndoRecordSerialize(t *testing.T) {
	t.Run("Serialize の出力を Deserialize でラウンドトリップできる", func(t *testing.T) {
		// GIVEN
		original := NewMetaInsertUndoRecord(MetaTableTypeIndexKeyColumn, []byte("primary_key"))

		// WHEN
		buf := original.Serialize()
		got, err := DeserializeMetaInsertUndoRecord(buf)

		// THEN
		assert.NoError(t, err)
		assert.Equal(t, original.MetaTableType(), got.MetaTableType())
		assert.Equal(t, original.Key(), got.Key())
	})

	t.Run("空 key でもラウンドトリップできる", func(t *testing.T) {
		// GIVEN
		original := NewMetaInsertUndoRecord(MetaTableTypeColumn, nil)

		// WHEN
		buf := original.Serialize()
		got, err := DeserializeMetaInsertUndoRecord(buf)

		// THEN
		assert.NoError(t, err)
		assert.Equal(t, MetaTableTypeColumn, got.MetaTableType())
		assert.Empty(t, got.Key())
	})

	t.Run("Header + Key の順に連結される", func(t *testing.T) {
		// GIVEN
		record := NewMetaInsertUndoRecord(MetaTableTypeConstraint, []byte{0xAB, 0xCD})

		// WHEN
		buf := record.Serialize()

		// THEN
		assert.Len(t, buf, metaInsertHeaderSize+2)
		assert.Equal(t, byte(MetaTableTypeConstraint), buf[0])
		assert.Equal(t, byte(0x00), buf[1])
		assert.Equal(t, byte(0x02), buf[2])
		assert.Equal(t, []byte{0xAB, 0xCD}, buf[3:])
	})
}

func TestDeserializeMetaInsertUndoRecord(t *testing.T) {
	t.Run("バッファが header サイズ未満の場合 ErrInvalidMetaInsertUndoRecord", func(t *testing.T) {
		// GIVEN
		buf := []byte{0x01, 0x00}

		// WHEN
		_, err := DeserializeMetaInsertUndoRecord(buf)

		// THEN
		assert.ErrorIs(t, err, ErrInvalidMetaInsertUndoRecord)
	})

	t.Run("Header が示す key 長に満たない場合 ErrInvalidMetaInsertUndoRecord", func(t *testing.T) {
		// GIVEN
		buf := []byte{byte(MetaTableTypeTable), 0x00, 0x05, 0xAA, 0xBB}

		// WHEN
		_, err := DeserializeMetaInsertUndoRecord(buf)

		// THEN
		assert.ErrorIs(t, err, ErrInvalidMetaInsertUndoRecord)
	})
}
