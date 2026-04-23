# MySQL プロトコル

## 参考文献

- [MySQLのプロトコルを学ぶ](https://asnokaze.hatenablog.com/entry/20141227/1419697189)

## パケット

- 通信メッセージの単位

| サイズ | 名前 | 説明 |
| --- | --- | --- |
| 3 バイト | パケット長 | パケットの長さ |
| 1 バイト | シーケンス ID | パケットのシーケンス ID |
| 可変長 | ペイロード | パケットの内容 |

## 流れ

1. Server Greeting (Initial Handshake)
2. Login Request (Handshake Response)
3. Response OK
4. Request Query (Com Query)
5. Response Query (Com Query Response)
6. Request Quit (Com Quit)

## Server Greeting

- クライアントがサーバーに接続すると、サーバーはクライアントに対してハンドシェイク・パケットを送信する
- ハンドシェイク・パケットには以下の情報が含まれる
  - 参考: https://dev.mysql.com/doc/dev/mysql-server/8.4.8/page_protocol_connection_phase_packets_protocol_handshake_v10.html

| 名前 | 説明 |
| --- | --- |
| Protocol Version | プロトコルのバージョン |
| Server Version | サーバーのバージョン |
| Thread ID | スレッド ID |
| Character Set | 文字セット 現状は UTF-8 のみ |

## Login Request (Handshake Response)

- Initial Handshake に対するクライアントの応答
- クライアントは以下の情報をサーバーに送信する

## Response OK

## Request Query (Com Query)

## Response Query (Com Query Response)

## Request Quit (Com Quit)
