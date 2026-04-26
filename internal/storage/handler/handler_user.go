package handler

import (
	"fmt"

	"minesql/internal/storage/dictionary"
)

// CreateUser は初期ユーザーをカタログに作成する
func (h *Handler) CreateUser(username, host string, authString [32]byte) error {
	userMeta := &dictionary.UserMeta{
		MetaPageId: h.Catalog.UserMetaPageId,
		Username:   username,
		Host:       host,
		AuthString: authString,
	}
	if err := userMeta.Insert(h.BufferPool); err != nil {
		return err
	}
	h.Catalog.Users = append(h.Catalog.Users, userMeta)
	return nil
}

// UpdateUser はカタログ上のユーザーの認証情報を更新する
func (h *Handler) UpdateUser(username, host string, authString [32]byte) error {
	user, ok := h.Catalog.GetUserByName(username)
	if !ok {
		return fmt.Errorf("user '%s' not found", username)
	}

	// B+Tree 上のレコードを更新
	user.Host = host
	user.AuthString = authString
	if err := user.Update(h.BufferPool); err != nil {
		return err
	}

	return nil
}
