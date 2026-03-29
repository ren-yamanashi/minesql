package server

import "minesql/internal/undo"

// session はクライアントごとの接続状態を管理する
type session struct {
	trxId undo.TrxId
}

func newSession() *session {
	return &session{}
}
