package client

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"os"
	"strings"
)

type Client struct {
	Address string
	Port    int
}

func NewClient(address string, port int) *Client {
	return &Client{
		Address: address,
		Port:    port,
	}
}

func (c *Client) Start() error {
	conn, err := net.Dial("tcp", net.JoinHostPort(c.Address, fmt.Sprintf("%d", c.Port)))
	if err != nil {
		return fmt.Errorf("failed to connect to server: %w\n", err)
	}
	defer conn.Close()

	fmt.Println("Connected to MineSQL Server!")
	reader := bufio.NewReader(os.Stdin)

	for {
		// ユーザー入力を受け取る (`;` が来るまで複数行を受け付ける)
		text := c.readMultilineInput(reader)
		if text == "" {
			continue
		}

		// exit コマンドで終了
		if text == "exit" {
			fmt.Println("Bye!")
			return nil
		}

		// サーバーに送信
		if err := c.writePacket(conn, text); err != nil {
			return fmt.Errorf("write error: %w\n", err)
		}

		// サーバーからの結果を受信
		response, err := c.readPacket(conn)
		if err != nil {
			return fmt.Errorf("read error: %w\n", err)
		}
		fmt.Println(response)
	}
}

// `;` が来るまで複数行の入力を受け付ける
func (c *Client) readMultilineInput(reader *bufio.Reader) string {
	var lines []string
	firstLine := true

	for {
		// プロンプトを表示
		if firstLine {
			fmt.Print("minesql> ")
			firstLine = false
		} else {
			fmt.Print("      -> ")
		}

		// 1行読み取る
		line, _ := reader.ReadString('\n')
		line = strings.TrimRight(line, "\n\r")
		if line == "exit" {
			return "exit"
		}

		// 空行はスキップ (最初の行の場合は処理を中断)
		if line == "" && len(lines) == 0 {
			return ""
		}

		lines = append(lines, line)

		// `;` が含まれていたら入力を終了
		if strings.Contains(line, ";") {
			break
		}
	}

	return strings.Join(lines, "\n")
}

// [Header 4 bytes][Body N bytes] 形式でパケットを送受信する
func (c *Client) writePacket(conn net.Conn, data string) error {
	bytes := []byte(data)
	length := uint32(len(bytes))
	header := make([]byte, 4)
	binary.BigEndian.PutUint32(header, length)
	_, err := conn.Write(append(header, bytes...))
	return err
}

// [Header 4 bytes][Body N bytes] 形式でパケットを送受信する
func (c *Client) readPacket(conn net.Conn) (string, error) {
	header := make([]byte, 4)
	if _, err := io.ReadFull(conn, header); err != nil {
		return "", err
	}
	length := binary.BigEndian.Uint32(header)
	body := make([]byte, length)
	if _, err := io.ReadFull(conn, body); err != nil {
		return "", err
	}
	return string(body), nil
}
