package executor

import "minesql/internal/storage/handler"

// AlterUser はユーザーの認証情報を更新する
type AlterUser struct {
	username   string
	host       string
	authString [32]byte
}

func NewAlterUser(username, host string, authString [32]byte) *AlterUser {
	return &AlterUser{
		username:   username,
		host:       host,
		authString: authString,
	}
}

func (au *AlterUser) Next() (Record, error) {
	hdl := handler.Get()
	if err := hdl.UpdateUser(au.username, au.host, au.authString); err != nil {
		return nil, err
	}
	return nil, nil
}
