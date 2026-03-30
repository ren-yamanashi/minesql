package server

import "minesql/internal/storage/engine"

// session はクライアントごとの接続状態を管理する
type session struct {
	trxId engine.TrxId
}

func newSession() *session {
	return &session{}
}
