package planner

import (
	"crypto/sha256"
	"minesql/internal/ast"
	"minesql/internal/executor"
	"minesql/internal/storage/handler"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestPlanAlterUser(t *testing.T) {
	t.Run("存在するユーザーに対して AlterUser executor を返す", func(t *testing.T) {
		// GIVEN
		initStorageManagerForTest(t)
		defer handler.Reset()

		hdl := handler.Get()
		authString := computePlannerTestAuthString("oldpass")
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

func computePlannerTestAuthString(password string) [32]byte {
	stage1 := sha256.Sum256([]byte(password))
	return sha256.Sum256(stage1[:])
}
