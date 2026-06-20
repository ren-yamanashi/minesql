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
