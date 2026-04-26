package server

import "minesql/internal/storage/handler"

// session はクライアントごとの接続状態を管理する
type session struct {
	trxId      handler.TrxId // 現在のトランザクション ID
	username   string        // 認証時に設定
	capability uint32        // クライアントとのネゴシエーション結果 (共通 capability)
}

func newSession(username string, capability uint32) *session {
	return &session{
		username:   username,
		capability: capability,
	}
}
