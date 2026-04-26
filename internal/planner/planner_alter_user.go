package planner

import (
	"fmt"

	"github.com/ren-yamanashi/minesql/internal/ast"
	"github.com/ren-yamanashi/minesql/internal/executor"
	"github.com/ren-yamanashi/minesql/internal/storage/acl"
	"github.com/ren-yamanashi/minesql/internal/storage/handler"
)

// PlanAlterUser は ALTER USER 文のバリデーションを行い、AlterUser executor を構築する
func PlanAlterUser(stmt *ast.AlterUserStmt) (executor.Executor, error) {
	hdl := handler.Get()

	// 対象ユーザーがカタログに存在するか検証
	_, ok := hdl.Catalog.GetUserByName(stmt.Username)
	if !ok {
		return nil, fmt.Errorf("user '%s' not found", stmt.Username)
	}

	// パスワードからソルト付きハッシュを計算
	authString, err := acl.CryptPassword(stmt.Password)
	if err != nil {
		return nil, fmt.Errorf("failed to hash password: %w", err)
	}

	return executor.NewAlterUser(stmt.Username, stmt.Host, authString), nil
}
