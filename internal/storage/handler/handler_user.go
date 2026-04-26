package handler

import (
	"fmt"

	"minesql/internal/storage/acl"
	"minesql/internal/storage/dictionary"
)

// LoadACL はカタログからユーザーを読み込んで ACL を構築する
//
// ユーザーが存在しない場合は false を返す
func (h *Handler) LoadACL() bool {
	if !h.Catalog.HasUsers() {
		return false
	}
	user := h.Catalog.Users[0]
	h.ACL = acl.NewACLFromCatalog(user.Username, user.Host, user.AuthString)
	return true
}

// CreateInitialUser は初期ユーザーを作成し、生成したランダムパスワードを返す
func (h *Handler) CreateInitialUser(username, host string) (string, error) {
	password, err := acl.GeneratePassword()
	if err != nil {
		return "", fmt.Errorf("failed to generate password: %w", err)
	}

	authString, err := acl.CryptPassword(password)
	if err != nil {
		return "", fmt.Errorf("failed to hash password: %w", err)
	}
	if err := h.CreateUser(username, host, authString); err != nil {
		return "", err
	}

	return password, nil
}

// CreateUser はユーザーをカタログに作成し、ACL を構築する
func (h *Handler) CreateUser(username, host string, authString string) error {
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
	h.ACL = acl.NewACLFromCatalog(username, host, authString)
	return nil
}

// UpdateUser はカタログ上のユーザーの認証情報を更新し、ACL を再構築する
func (h *Handler) UpdateUser(username, host string, authString string) error {
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

	// ACL を再構築 (Hash Entry Cache は新しい ACL でクリアされる)
	h.ACL = acl.NewACLFromCatalog(username, host, authString)
	return nil
}
