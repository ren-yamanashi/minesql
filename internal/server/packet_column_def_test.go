package server

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestColumnDefPacketBuild(t *testing.T) {
	t.Run("カラム定義パケットを構築できる", func(t *testing.T) {
		// GIVEN
		col := &columnDefPacket{tableName: "users", name: "id"}

		// WHEN
		buf := col.build()

		// THEN
		pos := buf

		// catalog: "def"
		catalog, pos, err := readLenEncString(pos)
		require.NoError(t, err)
		assert.Equal(t, "def", catalog)

		// schema: ""
		schema, pos, err := readLenEncString(pos)
		require.NoError(t, err)
		assert.Equal(t, "", schema)

		// table
		table, pos, err := readLenEncString(pos)
		require.NoError(t, err)
		assert.Equal(t, "users", table)

		// org_table
		orgTable, pos, err := readLenEncString(pos)
		require.NoError(t, err)
		assert.Equal(t, "users", orgTable)

		// name
		name, pos, err := readLenEncString(pos)
		require.NoError(t, err)
		assert.Equal(t, "id", name)

		// org_name
		orgName, pos, err := readLenEncString(pos)
		require.NoError(t, err)
		assert.Equal(t, "id", orgName)

		// fixed_fields_length: 0x0c
		fixedLen, pos, err := readLenEncInt(pos)
		require.NoError(t, err)
		assert.Equal(t, uint64(0x0c), fixedLen)

		// character_set: utf8mb4_general_ci
		assert.Equal(t, uint16(charsetUTF8MB4), readUint16(pos[:2]))
		pos = pos[2:]

		// column_length: 255
		assert.Equal(t, uint32(255), readUint32(pos[:4]))
		pos = pos[4:]

		// column_type: MYSQL_TYPE_VAR_STRING
		assert.Equal(t, byte(0xFD), pos[0])
		pos = pos[1:]

		// flags
		assert.Equal(t, uint16(0), readUint16(pos[:2]))
		pos = pos[2:]

		// decimals
		assert.Equal(t, byte(0), pos[0])
		pos = pos[1:]

		// filler
		assert.Equal(t, []byte{0x00, 0x00}, pos[:2])
	})
}

func TestColumnDefPacketImplementsPacket(t *testing.T) {
	t.Run("packet interface を実装している", func(t *testing.T) {
		// GIVEN
		var p packet = &columnDefPacket{}

		// THEN
		assert.NotNil(t, p)
	})
}
