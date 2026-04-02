package server

import "minesql/internal/storage/handler"

// session はクライアントごとの接続状態を管理する
type session struct {
	trxId handler.TrxId
}

func newSession() *session {
	return &session{}
}
