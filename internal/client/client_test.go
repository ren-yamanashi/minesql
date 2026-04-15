package client

import (
	"bufio"
	"encoding/binary"
	"io"
	"net"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNewClient(t *testing.T) {
	t.Run("Client が正しく初期化される", func(t *testing.T) {
		// WHEN
		c := NewClient("127.0.0.1", 3307)

		// THEN
		assert.Equal(t, "127.0.0.1", c.address)
		assert.Equal(t, 3307, c.port)
	})
}

func TestReadMultilineInput(t *testing.T) {
	c := NewClient("localhost", 3307)

	t.Run("セミコロンで終わる 1 行入力を受け付ける", func(t *testing.T) {
		// GIVEN
		reader := bufio.NewReader(strings.NewReader("SELECT * FROM users;\n"))

		// WHEN
		result := c.readMultilineInput(reader)

		// THEN
		assert.Equal(t, "SELECT * FROM users;", result)
	})

	t.Run("セミコロンが来るまで複数行を受け付ける", func(t *testing.T) {
		// GIVEN
		input := "SELECT *\nFROM users\nWHERE id = 1;\n"
		reader := bufio.NewReader(strings.NewReader(input))

		// WHEN
		result := c.readMultilineInput(reader)

		// THEN
		assert.Equal(t, "SELECT *\nFROM users\nWHERE id = 1;", result)
	})

	t.Run("空行のみの場合は空文字列を返す", func(t *testing.T) {
		// GIVEN
		reader := bufio.NewReader(strings.NewReader("\n"))

		// WHEN
		result := c.readMultilineInput(reader)

		// THEN
		assert.Equal(t, "", result)
	})

	t.Run("exit を入力すると exit を返す", func(t *testing.T) {
		// GIVEN
		reader := bufio.NewReader(strings.NewReader("exit\n"))

		// WHEN
		result := c.readMultilineInput(reader)

		// THEN
		assert.Equal(t, "exit", result)
	})

	t.Run("exit; を入力すると exit を返す", func(t *testing.T) {
		// GIVEN
		reader := bufio.NewReader(strings.NewReader("exit;\n"))

		// WHEN
		result := c.readMultilineInput(reader)

		// THEN
		assert.Equal(t, "exit", result)
	})

	t.Run("複数行入力の途中で exit を入力すると exit を返す", func(t *testing.T) {
		// GIVEN: セミコロンを待っている途中で exit が来る
		input := "SELECT *\nexit\n"
		reader := bufio.NewReader(strings.NewReader(input))

		// WHEN
		result := c.readMultilineInput(reader)

		// THEN: exit チェックが行結合より先に評価される
		assert.Equal(t, "exit", result)
	})

	t.Run("セミコロンが行の途中にある場合はその行まで返す", func(t *testing.T) {
		// GIVEN: 1 行に複数のセミコロンを含む
		input := "SELECT * FROM users; DROP TABLE users;\n"
		reader := bufio.NewReader(strings.NewReader(input))

		// WHEN
		result := c.readMultilineInput(reader)

		// THEN: セミコロンを含む行全体が返される
		assert.Equal(t, "SELECT * FROM users; DROP TABLE users;", result)
	})
}

func TestWriteAndReadPacket(t *testing.T) {
	t.Run("パケットの書き込みと読み込みが対称的に動作する", func(t *testing.T) {
		// GIVEN
		c := NewClient("localhost", 3307)
		server, client := net.Pipe()
		defer func() { _ = server.Close() }()
		defer func() { _ = client.Close() }()

		// WHEN: クライアント側でパケットを書き込む
		go func() {
			err := c.writePacket(client, "SELECT * FROM users;")
			assert.NoError(t, err)
		}()

		// THEN: サーバー側でパケットを読み込める
		header := make([]byte, 4)
		_, err := io.ReadFull(server, header)
		assert.NoError(t, err)

		length := binary.BigEndian.Uint32(header)
		body := make([]byte, length)
		_, err = io.ReadFull(server, body)
		assert.NoError(t, err)
		assert.Equal(t, "SELECT * FROM users;", string(body))
	})

	t.Run("readPacket でサーバーからのレスポンスを読み込める", func(t *testing.T) {
		// GIVEN
		c := NewClient("localhost", 3307)
		server, client := net.Pipe()
		defer func() { _ = server.Close() }()
		defer func() { _ = client.Close() }()

		// サーバー側でパケットを書き込む
		go func() {
			data := []byte("1,Alice\n2,Bob\n")
			packet := make([]byte, 4+len(data))
			binary.BigEndian.PutUint32(packet[0:4], uint32(len(data)))
			copy(packet[4:], data)
			_, err := server.Write(packet)
			assert.NoError(t, err)
		}()

		// WHEN
		result, err := c.readPacket(client)

		// THEN
		assert.NoError(t, err)
		assert.Equal(t, "1,Alice\n2,Bob\n", result)
	})

	t.Run("空文字列のパケットを送受信できる", func(t *testing.T) {
		// GIVEN
		c := NewClient("localhost", 3307)
		server, client := net.Pipe()
		defer func() { _ = server.Close() }()
		defer func() { _ = client.Close() }()

		go func() {
			err := c.writePacket(client, "")
			assert.NoError(t, err)
		}()

		// WHEN
		header := make([]byte, 4)
		_, err := io.ReadFull(server, header)
		assert.NoError(t, err)

		// THEN
		length := binary.BigEndian.Uint32(header)
		assert.Equal(t, uint32(0), length)
	})

	t.Run("writePacket で書いたデータを readPacket で読める", func(t *testing.T) {
		// GIVEN
		c := NewClient("localhost", 3307)
		server, client := net.Pipe()
		defer func() { _ = server.Close() }()
		defer func() { _ = client.Close() }()

		go func() {
			err := c.writePacket(client, "hello world")
			assert.NoError(t, err)
		}()

		// WHEN
		result, err := c.readPacket(server)

		// THEN
		assert.NoError(t, err)
		assert.Equal(t, "hello world", result)
	})

	t.Run("readPacket でコネクションが切断された場合はエラーを返す", func(t *testing.T) {
		// GIVEN
		c := NewClient("localhost", 3307)
		server, client := net.Pipe()
		_ = server.Close() // 先に閉じる
		defer func() { _ = client.Close() }()

		// WHEN
		_, err := c.readPacket(client)

		// THEN
		assert.Error(t, err)
	})

	t.Run("日本語を含むパケットを送受信できる", func(t *testing.T) {
		// GIVEN
		c := NewClient("localhost", 3307)
		server, client := net.Pipe()
		defer func() { _ = server.Close() }()
		defer func() { _ = client.Close() }()

		go func() {
			err := c.writePacket(client, "こんにちは世界")
			assert.NoError(t, err)
		}()

		// WHEN
		result, err := c.readPacket(server)

		// THEN
		assert.NoError(t, err)
		assert.Equal(t, "こんにちは世界", result)
	})
}
