package server

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"minesql/internal/executor"
	"minesql/internal/storage/handler"
)

func TestOnComQuery(t *testing.T) {
	t.Run("DDL は OK_Packet を返す", func(t *testing.T) {
		// GIVEN
		s := setupTestServer(t)
		defer handler.Reset()
		serverConn, clientConn := createConnPair(t)
		sess := newSession("", 0)

		// WHEN
		go func() {
			s.onComQuery(serverConn, sess, "CREATE TABLE hcq_ddl (id VARCHAR, PRIMARY KEY (id));")
		}()

		// THEN
		resp, err := clientConn.readPacket()
		require.NoError(t, err)
		assert.Equal(t, byte(0x00), resp[0]) // OK_Packet
	})

	t.Run("INSERT は affected_rows 付きの OK_Packet を返す", func(t *testing.T) {
		// GIVEN
		s := setupTestServer(t)
		defer handler.Reset()
		sess := newSession("", 0)

		_, err := s.executeQuery(sess, "CREATE TABLE hcq_ins (id VARCHAR, name VARCHAR, PRIMARY KEY (id));")
		require.NoError(t, err)

		serverConn, clientConn := createConnPair(t)

		// WHEN
		go func() {
			s.onComQuery(serverConn, sess, "INSERT INTO hcq_ins (id, name) VALUES ('1', 'Alice');")
		}()

		// THEN
		resp, err := clientConn.readPacket()
		require.NoError(t, err)
		assert.Equal(t, byte(0x00), resp[0]) // OK_Packet
	})

	t.Run("SELECT は結果セットを返す", func(t *testing.T) {
		// GIVEN
		s := setupTestServer(t)
		defer handler.Reset()
		sess := newSession("", 0)

		_, err := s.executeQuery(sess, "CREATE TABLE hcq_sel (id VARCHAR, name VARCHAR, PRIMARY KEY (id));")
		require.NoError(t, err)
		_, err = s.executeQuery(sess, "INSERT INTO hcq_sel (id, name) VALUES ('1', 'Alice');")
		require.NoError(t, err)

		serverConn, clientConn := createConnPair(t)

		// WHEN
		go func() {
			s.onComQuery(serverConn, sess, "SELECT * FROM hcq_sel;")
		}()

		// THEN
		// 1. Column Count パケット
		colCount, err := clientConn.readPacket()
		require.NoError(t, err)
		count, _, err := readLenEncInt(colCount)
		require.NoError(t, err)
		assert.Equal(t, uint64(2), count) // id, name の 2 カラム

		// 2. Column Definition パケット (2 つ)
		colDef1, err := clientConn.readPacket()
		require.NoError(t, err)
		assert.NotEmpty(t, colDef1)

		colDef2, err := clientConn.readPacket()
		require.NoError(t, err)
		assert.NotEmpty(t, colDef2)

		// 3. EOF_Packet (Column Definition の終了)
		eofPkt1, err := clientConn.readPacket()
		require.NoError(t, err)
		assert.Equal(t, byte(0xFE), eofPkt1[0])

		// 4. Row パケット (1 行)
		row, err := clientConn.readPacket()
		require.NoError(t, err)
		val1, rest, err := readLenEncString(row)
		require.NoError(t, err)
		assert.Equal(t, "1", val1)
		val2, _, err := readLenEncString(rest)
		require.NoError(t, err)
		assert.Equal(t, "Alice", val2)

		// 5. EOF_Packet (結果セットの終了)
		eofPkt2, err := clientConn.readPacket()
		require.NoError(t, err)
		assert.Equal(t, byte(0xFE), eofPkt2[0])
	})

	t.Run("不正な SQL は ERR_Packet を返す", func(t *testing.T) {
		// GIVEN
		s := setupTestServer(t)
		defer handler.Reset()
		serverConn, clientConn := createConnPair(t)
		sess := newSession("", 0)

		// WHEN
		go func() {
			s.onComQuery(serverConn, sess, "INVALID SQL;")
		}()

		// THEN
		resp, err := clientConn.readPacket()
		require.NoError(t, err)
		assert.Equal(t, byte(0xFF), resp[0]) // ERR_Packet
	})

	t.Run("SET 文は OK_Packet を返す", func(t *testing.T) {
		// GIVEN
		s := setupTestServer(t)
		defer handler.Reset()
		serverConn, clientConn := createConnPair(t)
		sess := newSession("", 0)

		// WHEN
		go func() {
			s.onComQuery(serverConn, sess, "SET NAMES utf8mb4;")
		}()

		// THEN
		resp, err := clientConn.readPacket()
		require.NoError(t, err)
		assert.Equal(t, byte(0x00), resp[0]) // OK_Packet
	})

	t.Run("トランザクション中は status_flags に serverStatusInTrans が設定される", func(t *testing.T) {
		// GIVEN
		s := setupTestServer(t)
		defer handler.Reset()
		sess := newSession("", 0)

		_, err := s.executeQuery(sess, "CREATE TABLE hcq_tx (id VARCHAR, PRIMARY KEY (id));")
		require.NoError(t, err)
		_, err = s.executeQuery(sess, "BEGIN;")
		require.NoError(t, err)

		serverConn, clientConn := createConnPair(t)

		// WHEN
		go func() {
			s.onComQuery(serverConn, sess, "INSERT INTO hcq_tx (id) VALUES ('1');")
		}()

		// THEN
		resp, err := clientConn.readPacket()
		require.NoError(t, err)
		assert.Equal(t, byte(0x00), resp[0])
		assert.Equal(t, serverStatusInTrans, readUint16(resp[3:5]))

		// クリーンアップ
		_, _ = s.executeQuery(sess, "ROLLBACK;")
	})

	t.Run("SELECT 結果が 0 行の場合も結果セットを返す (Row パケットなし)", func(t *testing.T) {
		// GIVEN
		s := setupTestServer(t)
		defer handler.Reset()
		sess := newSession("", 0)

		_, err := s.executeQuery(sess, "CREATE TABLE hcq_empty (id VARCHAR, PRIMARY KEY (id));")
		require.NoError(t, err)

		serverConn, clientConn := createConnPair(t)

		// WHEN
		go func() {
			s.onComQuery(serverConn, sess, "SELECT * FROM hcq_empty;")
		}()

		// THEN
		// 1. Column Count
		colCount, err := clientConn.readPacket()
		require.NoError(t, err)
		count, _, err := readLenEncInt(colCount)
		require.NoError(t, err)
		assert.Equal(t, uint64(1), count) // id のみ

		// 2. Column Definition (1 つ)
		_, err = clientConn.readPacket()
		require.NoError(t, err)

		// 3. EOF_Packet (Column Definition の終了)
		eofPkt1, err := clientConn.readPacket()
		require.NoError(t, err)
		assert.Equal(t, byte(0xFE), eofPkt1[0])

		// 4. Row パケットなし → 直接 EOF_Packet (結果セットの終了)
		eofPkt2, err := clientConn.readPacket()
		require.NoError(t, err)
		assert.Equal(t, byte(0xFE), eofPkt2[0])
	})
}

func TestBuildRowPacket(t *testing.T) {
	t.Run("行パケットを構築できる", func(t *testing.T) {
		// GIVEN
		record := executor.Record{[]byte("1"), []byte("Alice")}

		// WHEN
		buf := buildRowPacket(record)

		// THEN
		val1, rest, err := readLenEncString(buf)
		require.NoError(t, err)
		assert.Equal(t, "1", val1)

		val2, rest, err := readLenEncString(rest)
		require.NoError(t, err)
		assert.Equal(t, "Alice", val2)

		assert.Empty(t, rest)
	})

	t.Run("空のレコード", func(t *testing.T) {
		// GIVEN
		record := executor.Record{}

		// WHEN
		buf := buildRowPacket(record)

		// THEN
		assert.Empty(t, buf)
	})
}
