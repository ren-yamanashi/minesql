package handler

import "minesql/internal/storage/dictionary"

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
