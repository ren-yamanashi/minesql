package planner

import (
	"testing"

	"github.com/ren-yamanashi/minesql/internal/ast"
	"github.com/ren-yamanashi/minesql/internal/executor"
	"github.com/ren-yamanashi/minesql/internal/storage/acl"
	"github.com/ren-yamanashi/minesql/internal/storage/handler"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPlanAlterUser(t *testing.T) {
	t.Run("存在するユーザーに対して AlterUser executor を返す", func(t *testing.T) {
		// GIVEN
		initStorageManagerForTest(t)
		defer handler.Reset()

		hdl := handler.Get()
		authString := cryptPlannerTestPassword(t, "oldpass")
		err := hdl.CreateUser("root", "%", authString)
		assert.NoError(t, err)

		stmt := &ast.AlterUserStmt{
			Username: "root",
			Host:     "%",
			Password: "newpass",
		}

		// WHEN
		exec, err := PlanAlterUser(stmt)

		// THEN
		assert.NoError(t, err)
		assert.NotNil(t, exec)
		assert.IsType(t, &executor.AlterUser{}, exec)
	})

	t.Run("存在しないユーザーの場合エラーを返す", func(t *testing.T) {
		// GIVEN
		initStorageManagerForTest(t)
		defer handler.Reset()

		stmt := &ast.AlterUserStmt{
			Username: "nonexistent",
			Host:     "%",
			Password: "pass",
		}

		// WHEN
		exec, err := PlanAlterUser(stmt)

		// THEN
		assert.Error(t, err)
		assert.Nil(t, exec)
		assert.Contains(t, err.Error(), "user 'nonexistent' not found")
	})
}

func cryptPlannerTestPassword(t *testing.T, password string) string {
	t.Helper()
	s, err := acl.CryptPassword(password)
	require.NoError(t, err)
	return s
}
