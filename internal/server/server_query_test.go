package server

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"minesql/internal/storage/acl"
	"minesql/internal/storage/handler"
)

func TestExecuteQuery(t *testing.T) {
	t.Run("CREATE TABLE を実行できる", func(t *testing.T) {
		// GIVEN
		s := setupTestServer(t)
		defer handler.Reset()
		sess := newSession("", 0)

		// WHEN
		result, err := s.onQuery(sess, "CREATE TABLE users (id VARCHAR, name VARCHAR, PRIMARY KEY (id));")

		// THEN
		assert.NoError(t, err)
		assert.Equal(t, resultOK, result.resultType)
	})

	t.Run("INSERT と SELECT を実行できる", func(t *testing.T) {
		// GIVEN
		s := setupTestServer(t)
		defer handler.Reset()
		sess := newSession("", 0)

		_, err := s.onQuery(sess, "CREATE TABLE users (id VARCHAR, name VARCHAR, PRIMARY KEY (id));")
		assert.NoError(t, err)

		// WHEN
		_, err = s.onQuery(sess, "INSERT INTO users (id, name) VALUES ('1', 'Alice'), ('2', 'Bob');")
		assert.NoError(t, err)

		result, err := s.onQuery(sess, "SELECT * FROM users;")

		// THEN
		assert.NoError(t, err)
		assert.Equal(t, resultResultSet, result.resultType)
		csv := resultToCSV(result)
		assert.Contains(t, csv, "1,Alice")
		assert.Contains(t, csv, "2,Bob")
	})

	t.Run("UPDATE を実行できる", func(t *testing.T) {
		// GIVEN
		s := setupTestServer(t)
		defer handler.Reset()
		sess := newSession("", 0)

		_, err := s.onQuery(sess, "CREATE TABLE users (id VARCHAR, name VARCHAR, PRIMARY KEY (id));")
		assert.NoError(t, err)
		_, err = s.onQuery(sess, "INSERT INTO users (id, name) VALUES ('1', 'Alice');")
		assert.NoError(t, err)

		// WHEN
		_, err = s.onQuery(sess, "UPDATE users SET name = 'Carol' WHERE id = '1';")
		assert.NoError(t, err)

		result, err := s.onQuery(sess, "SELECT * FROM users;")

		// THEN
		assert.NoError(t, err)
		csv := resultToCSV(result)
		assert.Contains(t, csv, "1,Carol")
	})

	t.Run("DELETE を実行できる", func(t *testing.T) {
		// GIVEN
		s := setupTestServer(t)
		defer handler.Reset()
		sess := newSession("", 0)

		_, err := s.onQuery(sess, "CREATE TABLE users (id VARCHAR, name VARCHAR, PRIMARY KEY (id));")
		assert.NoError(t, err)
		_, err = s.onQuery(sess, "INSERT INTO users (id, name) VALUES ('1', 'Alice'), ('2', 'Bob');")
		assert.NoError(t, err)

		// WHEN
		_, err = s.onQuery(sess, "DELETE FROM users WHERE id = '1';")
		assert.NoError(t, err)

		result, err := s.onQuery(sess, "SELECT * FROM users;")

		// THEN
		assert.NoError(t, err)
		csv := resultToCSV(result)
		assert.NotContains(t, csv, "Alice")
		assert.Contains(t, csv, "2,Bob")
	})

	t.Run("BEGIN なしの DML は autocommit される", func(t *testing.T) {
		// GIVEN
		s := setupTestServer(t)
		defer handler.Reset()
		sess := newSession("", 0)

		_, err := s.onQuery(sess, "CREATE TABLE users (id VARCHAR, name VARCHAR, PRIMARY KEY (id));")
		assert.NoError(t, err)

		// WHEN: BEGIN なしで INSERT
		_, err = s.onQuery(sess, "INSERT INTO users (id, name) VALUES ('1', 'Alice');")
		assert.NoError(t, err)

		// THEN: trxId は 0 のまま (autocommit 済み)
		assert.Equal(t, handler.TrxId(0), sess.trxId)

		// THEN: データは永続化されている
		result, err := s.onQuery(sess, "SELECT * FROM users;")
		assert.NoError(t, err)
		csv := resultToCSV(result)
		assert.Contains(t, csv, "1,Alice")
	})

	t.Run("autocommit のデータは ROLLBACK で取り消せない", func(t *testing.T) {
		// GIVEN: BEGIN なしで INSERT (autocommit)
		s := setupTestServer(t)
		defer handler.Reset()
		sess := newSession("", 0)

		_, err := s.onQuery(sess, "CREATE TABLE users (id VARCHAR, name VARCHAR, PRIMARY KEY (id));")
		assert.NoError(t, err)
		_, err = s.onQuery(sess, "INSERT INTO users (id, name) VALUES ('1', 'Alice');")
		assert.NoError(t, err)

		// WHEN: その後 BEGIN → ROLLBACK しても autocommit 済みのデータは残る
		_, err = s.onQuery(sess, "BEGIN;")
		assert.NoError(t, err)
		_, err = s.onQuery(sess, "INSERT INTO users (id, name) VALUES ('2', 'Bob');")
		assert.NoError(t, err)
		_, err = s.onQuery(sess, "ROLLBACK;")
		assert.NoError(t, err)

		// THEN: autocommit の Alice は残り、トランザクション内の Bob は消える
		result, err := s.onQuery(sess, "SELECT * FROM users;")
		assert.NoError(t, err)
		csv := resultToCSV(result)
		assert.Contains(t, csv, "1,Alice")
		assert.NotContains(t, csv, "Bob")
	})

	t.Run("不正な SQL はエラーを返す", func(t *testing.T) {
		// GIVEN
		s := setupTestServer(t)
		defer handler.Reset()
		sess := newSession("", 0)

		// WHEN
		_, err := s.onQuery(sess, "INVALID SQL;")

		// THEN
		assert.Error(t, err)
	})

	t.Run("START TRANSACTION が BEGIN として処理される", func(t *testing.T) {
		// GIVEN
		s := setupTestServer(t)
		defer handler.Reset()
		sess := newSession("", 0)

		_, err := s.onQuery(sess, "CREATE TABLE users (id VARCHAR, name VARCHAR, PRIMARY KEY (id));")
		assert.NoError(t, err)

		// WHEN: START TRANSACTION でトランザクション開始
		result, err := s.onQuery(sess, "START TRANSACTION;")
		assert.NoError(t, err)
		assert.Equal(t, resultOK, result.resultType)
		assert.NotEqual(t, handler.TrxId(0), sess.trxId)

		// INSERT → ROLLBACK
		_, err = s.onQuery(sess, "INSERT INTO users (id, name) VALUES ('1', 'Alice');")
		assert.NoError(t, err)
		_, err = s.onQuery(sess, "ROLLBACK;")
		assert.NoError(t, err)

		// THEN: ROLLBACK されているのでテーブルが空
		selectResult, err := s.onQuery(sess, "SELECT * FROM users;")
		assert.NoError(t, err)
		assert.Empty(t, selectResult.records)
	})

	t.Run("START TRANSACTION はセミコロンなしでも処理される", func(t *testing.T) {
		// GIVEN
		s := setupTestServer(t)
		defer handler.Reset()
		sess := newSession("", 0)

		// WHEN: セミコロンなし (mysql クライアントが送る形式)
		result, err := s.onQuery(sess, "START TRANSACTION")
		assert.NoError(t, err)
		assert.Equal(t, resultOK, result.resultType)
		assert.NotEqual(t, handler.TrxId(0), sess.trxId)

		// クリーンアップ
		_, _ = s.onQuery(sess, "ROLLBACK;")
	})

	t.Run("SET 文は無視して OK を返す", func(t *testing.T) {
		// GIVEN
		s := setupTestServer(t)
		defer handler.Reset()
		sess := newSession("", 0)

		// WHEN
		result, err := s.onQuery(sess, "SET NAMES utf8mb4;")

		// THEN
		assert.NoError(t, err)
		assert.Equal(t, resultOK, result.resultType)
	})
}

func TestExecuteQueryAlterUser(t *testing.T) {
	t.Run("ALTER USER でパスワードを変更できる", func(t *testing.T) {
		// GIVEN
		s := setupTestServer(t)
		defer handler.Reset()
		sess := newSession("", 0)

		// 初期ユーザーを作成
		hdl := handler.Get()
		oldAuthString, err := acl.CryptPassword("oldpass")
		require.NoError(t, err)
		err = hdl.CreateUser("root", "%", oldAuthString)
		require.NoError(t, err)

		// WHEN
		result, err := s.onQuery(sess, "ALTER USER 'root'@'%' IDENTIFIED BY 'newpass';")

		// THEN
		assert.NoError(t, err)
		assert.Equal(t, resultOK, result.resultType)

		// カタログ上の AuthString が更新されている (ソルト付きハッシュ)
		user, ok := hdl.Catalog.GetUserByName("root")
		assert.True(t, ok)
		assert.True(t, acl.VerifyCryptPassword("newpass", user.AuthString))

		// ACL が再構築されている (新しいパスワードで検証できる)
		aclAuthString, ok := hdl.ACL.Lookup("%", "root")
		assert.True(t, ok)
		assert.True(t, acl.VerifyCryptPassword("newpass", aclAuthString))
	})

	t.Run("ALTER USER 後に別のクエリを実行できる", func(t *testing.T) {
		// GIVEN
		s := setupTestServer(t)
		defer handler.Reset()
		sess := newSession("", 0)

		hdl := handler.Get()
		oldAuthString, err := acl.CryptPassword("oldpass")
		require.NoError(t, err)
		err = hdl.CreateUser("root", "%", oldAuthString)
		require.NoError(t, err)

		// ALTER USER を実行
		_, err = s.onQuery(sess, "ALTER USER 'root'@'%' IDENTIFIED BY 'newpass';")
		require.NoError(t, err)

		// WHEN: ALTER USER 後に CREATE TABLE を実行
		result, err := s.onQuery(sess, "CREATE TABLE users (id VARCHAR, name VARCHAR, PRIMARY KEY (id));")

		// THEN
		assert.NoError(t, err)
		assert.Equal(t, resultOK, result.resultType)
	})

	t.Run("存在しないユーザーの ALTER USER はエラーになる", func(t *testing.T) {
		// GIVEN
		s := setupTestServer(t)
		defer handler.Reset()
		sess := newSession("", 0)

		// WHEN
		_, err := s.onQuery(sess, "ALTER USER 'nonexistent'@'%' IDENTIFIED BY 'pass';")

		// THEN
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "user 'nonexistent' not found")
	})
}

func TestExecuteQueryTransaction(t *testing.T) {
	t.Run("BEGIN で trxId が設定される", func(t *testing.T) {
		// GIVEN
		s := setupTestServer(t)
		defer handler.Reset()
		sess := newSession("", 0)
		assert.Equal(t, handler.TrxId(0), sess.trxId)

		// WHEN
		result, err := s.onQuery(sess, "BEGIN;")

		// THEN
		assert.NoError(t, err)
		assert.Equal(t, resultOK, result.resultType)
		assert.NotEqual(t, handler.TrxId(0), sess.trxId)
	})

	t.Run("COMMIT で trxId がリセットされる", func(t *testing.T) {
		// GIVEN
		s := setupTestServer(t)
		defer handler.Reset()
		sess := newSession("", 0)

		_, err := s.onQuery(sess, "BEGIN;")
		assert.NoError(t, err)

		_, err = s.onQuery(sess, "CREATE TABLE users (id VARCHAR, name VARCHAR, PRIMARY KEY (id));")
		assert.NoError(t, err)

		_, err = s.onQuery(sess, "INSERT INTO users (id, name) VALUES ('1', 'Alice');")
		assert.NoError(t, err)

		// WHEN
		result, err := s.onQuery(sess, "COMMIT;")

		// THEN
		assert.NoError(t, err)
		assert.Equal(t, resultOK, result.resultType)
		assert.Equal(t, handler.TrxId(0), sess.trxId)
	})

	t.Run("ROLLBACK で INSERT が取り消される", func(t *testing.T) {
		// GIVEN
		s := setupTestServer(t)
		defer handler.Reset()
		sess := newSession("", 0)

		_, err := s.onQuery(sess, "CREATE TABLE users (id VARCHAR, name VARCHAR, PRIMARY KEY (id));")
		assert.NoError(t, err)

		_, err = s.onQuery(sess, "BEGIN;")
		assert.NoError(t, err)

		_, err = s.onQuery(sess, "INSERT INTO users (id, name) VALUES ('1', 'Alice');")
		assert.NoError(t, err)

		// WHEN
		_, err = s.onQuery(sess, "ROLLBACK;")
		assert.NoError(t, err)

		// THEN: INSERT が取り消されてテーブルが空
		result, err := s.onQuery(sess, "SELECT * FROM users;")
		assert.NoError(t, err)
		assert.Empty(t, result.records)
	})

	t.Run("ROLLBACK で UPDATE が取り消される", func(t *testing.T) {
		// GIVEN
		s := setupTestServer(t)
		defer handler.Reset()
		sess := newSession("", 0)

		_, err := s.onQuery(sess, "CREATE TABLE users (id VARCHAR, name VARCHAR, PRIMARY KEY (id));")
		assert.NoError(t, err)
		_, err = s.onQuery(sess, "BEGIN;")
		assert.NoError(t, err)
		_, err = s.onQuery(sess, "INSERT INTO users (id, name) VALUES ('1', 'Alice');")
		assert.NoError(t, err)
		_, err = s.onQuery(sess, "COMMIT;")
		assert.NoError(t, err)

		_, err = s.onQuery(sess, "BEGIN;")
		assert.NoError(t, err)
		_, err = s.onQuery(sess, "UPDATE users SET name = 'Carol' WHERE id = '1';")
		assert.NoError(t, err)

		// WHEN
		_, err = s.onQuery(sess, "ROLLBACK;")
		assert.NoError(t, err)

		// THEN: UPDATE が取り消されて元の値
		result, err := s.onQuery(sess, "SELECT * FROM users;")
		assert.NoError(t, err)
		csv := resultToCSV(result)
		assert.Contains(t, csv, "1,Alice")
	})

	t.Run("ROLLBACK で DELETE が取り消される", func(t *testing.T) {
		// GIVEN
		s := setupTestServer(t)
		defer handler.Reset()
		sess := newSession("", 0)

		_, err := s.onQuery(sess, "CREATE TABLE users (id VARCHAR, name VARCHAR, PRIMARY KEY (id));")
		assert.NoError(t, err)
		_, err = s.onQuery(sess, "BEGIN;")
		assert.NoError(t, err)
		_, err = s.onQuery(sess, "INSERT INTO users (id, name) VALUES ('1', 'Alice');")
		assert.NoError(t, err)
		_, err = s.onQuery(sess, "COMMIT;")
		assert.NoError(t, err)

		_, err = s.onQuery(sess, "BEGIN;")
		assert.NoError(t, err)
		_, err = s.onQuery(sess, "DELETE FROM users WHERE id = '1';")
		assert.NoError(t, err)

		// WHEN
		_, err = s.onQuery(sess, "ROLLBACK;")
		assert.NoError(t, err)

		// THEN: DELETE が取り消されてレコードが復元
		result, err := s.onQuery(sess, "SELECT * FROM users;")
		assert.NoError(t, err)
		csv := resultToCSV(result)
		assert.Contains(t, csv, "1,Alice")
	})

	t.Run("COMMIT 後に新しいトランザクションを開始できる", func(t *testing.T) {
		// GIVEN
		s := setupTestServer(t)
		defer handler.Reset()
		sess := newSession("", 0)

		_, err := s.onQuery(sess, "CREATE TABLE users (id VARCHAR, name VARCHAR, PRIMARY KEY (id));")
		assert.NoError(t, err)

		// 1 回目のトランザクション: INSERT → COMMIT
		_, err = s.onQuery(sess, "BEGIN;")
		assert.NoError(t, err)
		_, err = s.onQuery(sess, "INSERT INTO users (id, name) VALUES ('1', 'Alice');")
		assert.NoError(t, err)
		_, err = s.onQuery(sess, "COMMIT;")
		assert.NoError(t, err)

		// 2 回目のトランザクション: INSERT → ROLLBACK
		_, err = s.onQuery(sess, "BEGIN;")
		assert.NoError(t, err)
		_, err = s.onQuery(sess, "INSERT INTO users (id, name) VALUES ('2', 'Bob');")
		assert.NoError(t, err)
		_, err = s.onQuery(sess, "ROLLBACK;")
		assert.NoError(t, err)

		// THEN: 1 回目の INSERT のみ残る
		result, err := s.onQuery(sess, "SELECT * FROM users;")
		assert.NoError(t, err)
		csv := resultToCSV(result)
		assert.Contains(t, csv, "1,Alice")
		assert.NotContains(t, csv, "Bob")
	})

	t.Run("トランザクション中に BEGIN を呼ぶとエラーになる", func(t *testing.T) {
		// GIVEN
		s := setupTestServer(t)
		defer handler.Reset()
		sess := newSession("", 0)

		_, err := s.onQuery(sess, "BEGIN;")
		assert.NoError(t, err)

		// WHEN
		_, err = s.onQuery(sess, "BEGIN;")

		// THEN
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "transaction already started")
	})

	t.Run("BEGIN なしで COMMIT を呼ぶとエラーになる", func(t *testing.T) {
		// GIVEN
		s := setupTestServer(t)
		defer handler.Reset()
		sess := newSession("", 0)

		// WHEN
		_, err := s.onQuery(sess, "COMMIT;")

		// THEN
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "no active transaction")
	})

	t.Run("BEGIN なしで ROLLBACK を呼ぶとエラーになる", func(t *testing.T) {
		// GIVEN
		s := setupTestServer(t)
		defer handler.Reset()
		sess := newSession("", 0)

		// WHEN
		_, err := s.onQuery(sess, "ROLLBACK;")

		// THEN
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "no active transaction")
	})

	t.Run("異なるセッションのトランザクションは独立している", func(t *testing.T) {
		// GIVEN
		s := setupTestServer(t)
		defer handler.Reset()
		sessA := newSession("", 0)
		sessB := newSession("", 0)

		_, err := s.onQuery(sessA, "CREATE TABLE users (id VARCHAR, name VARCHAR, PRIMARY KEY (id));")
		assert.NoError(t, err)

		// セッション A で BEGIN + INSERT
		_, err = s.onQuery(sessA, "BEGIN;")
		assert.NoError(t, err)
		_, err = s.onQuery(sessA, "INSERT INTO users (id, name) VALUES ('1', 'Alice');")
		assert.NoError(t, err)

		// WHEN: セッション B で BEGIN + ROLLBACK
		_, err = s.onQuery(sessB, "BEGIN;")
		assert.NoError(t, err)
		_, err = s.onQuery(sessB, "ROLLBACK;")
		assert.NoError(t, err)

		// THEN: セッション A のデータは影響を受けない
		_, err = s.onQuery(sessA, "COMMIT;")
		assert.NoError(t, err)

		result, err := s.onQuery(sessA, "SELECT * FROM users;")
		assert.NoError(t, err)
		csv := resultToCSV(result)
		assert.Contains(t, csv, "1,Alice")
	})

	t.Run("接続切断時にアクティブなトランザクションが自動ロールバックされる", func(t *testing.T) {
		// GIVEN: BEGIN → INSERT したが COMMIT していない
		s := setupTestServer(t)
		defer handler.Reset()
		sess := newSession("", 0)

		_, err := s.onQuery(sess, "CREATE TABLE users (id VARCHAR, name VARCHAR, PRIMARY KEY (id));")
		assert.NoError(t, err)

		_, err = s.onQuery(sess, "BEGIN;")
		assert.NoError(t, err)

		_, err = s.onQuery(sess, "INSERT INTO users (id, name) VALUES ('1', 'Alice');")
		assert.NoError(t, err)

		// WHEN: 接続切断をシミュレート
		assert.NotEqual(t, handler.TrxId(0), sess.trxId)
		err = handler.Get().RollbackTrx(sess.trxId)
		assert.NoError(t, err)
		sess.trxId = 0

		// THEN: INSERT がロールバックされてテーブルが空
		result, err := s.onQuery(sess, "SELECT * FROM users;")
		assert.NoError(t, err)
		assert.Empty(t, result.records)
	})

	t.Run("トランザクションなしの接続切断では何も起きない", func(t *testing.T) {
		// GIVEN: BEGIN していない
		s := setupTestServer(t)
		defer handler.Reset()
		sess := newSession("", 0)

		_, err := s.onQuery(sess, "CREATE TABLE users (id VARCHAR, name VARCHAR, PRIMARY KEY (id));")
		assert.NoError(t, err)
		_, err = s.onQuery(sess, "INSERT INTO users (id, name) VALUES ('1', 'Alice');")
		assert.NoError(t, err)

		// WHEN: 接続切断をシミュレート (trxId == 0 なのでロールバックは走らない)
		assert.Equal(t, handler.TrxId(0), sess.trxId)

		// THEN: データはそのまま残る
		result, err := s.onQuery(sess, "SELECT * FROM users;")
		assert.NoError(t, err)
		csv := resultToCSV(result)
		assert.Contains(t, csv, "1,Alice")
	})
}

func TestConnectionDisconnectReleasesLock(t *testing.T) {
	t.Run("接続切断時にアクティブなトランザクションのロックが解放される", func(t *testing.T) {
		// GIVEN
		s := setupTestServer(t)
		t.Setenv("MINESQL_LOCK_WAIT_TIMEOUT", "200")
		handler.Reset()
		handler.Init()
		defer handler.Reset()

		sess1 := newSession("", 0)

		// テーブル作成
		_, err := s.onQuery(sess1, "CREATE TABLE users (id VARCHAR, name VARCHAR, PRIMARY KEY (id));")
		assert.NoError(t, err)

		// sess1 でトランザクション開始 → INSERT (排他ロック取得)
		_, err = s.onQuery(sess1, "BEGIN;")
		assert.NoError(t, err)
		_, err = s.onQuery(sess1, "INSERT INTO users (id, name) VALUES ('1', 'Alice');")
		assert.NoError(t, err)

		// WHEN: sess1 の接続が切断される (auto-rollback をシミュレート)
		hdl := handler.Get()
		err = hdl.RollbackTrx(sess1.trxId)
		assert.NoError(t, err)
		sess1.trxId = 0

		// THEN: sess2 が同じ行を INSERT できる (ロックが解放されている)
		sess2 := newSession("", 0)
		_, err = s.onQuery(sess2, "BEGIN;")
		assert.NoError(t, err)
		_, err = s.onQuery(sess2, "INSERT INTO users (id, name) VALUES ('1', 'Bob');")
		assert.NoError(t, err)
		_, err = s.onQuery(sess2, "COMMIT;")
		assert.NoError(t, err)

		// データが Bob になっている
		sess3 := newSession("", 0)
		result, err := s.onQuery(sess3, "SELECT * FROM users;")
		assert.NoError(t, err)
		csv := resultToCSV(result)
		assert.Contains(t, csv, "1,Bob")
	})
}

func TestResolveColumnInfo(t *testing.T) {
	t.Run("SELECT * で全カラムが順序位置でソートされて返る", func(t *testing.T) {
		// GIVEN
		setupTestServer(t)
		defer handler.Reset()
		s := &Server{}
		sess := newSession("", 0)

		_, err := s.onQuery(sess, "CREATE TABLE rc_all (id VARCHAR, name VARCHAR, age VARCHAR, PRIMARY KEY (id));")
		require.NoError(t, err)

		result, err := s.onQuery(sess, "SELECT * FROM rc_all;")

		// THEN
		require.NoError(t, err)
		assert.Equal(t, resultResultSet, result.resultType)
		require.Len(t, result.columns, 3)
		assert.Equal(t, "id", result.columns[0].name)
		assert.Equal(t, "name", result.columns[1].name)
		assert.Equal(t, "age", result.columns[2].name)
		// テーブル名が設定されている
		assert.Equal(t, "rc_all", result.columns[0].tableName)
	})

	t.Run("カラム指定で指定したカラムのみ返る", func(t *testing.T) {
		// GIVEN
		setupTestServer(t)
		defer handler.Reset()
		s := &Server{}
		sess := newSession("", 0)

		_, err := s.onQuery(sess, "CREATE TABLE rc_cols (id VARCHAR, name VARCHAR, age VARCHAR, PRIMARY KEY (id));")
		require.NoError(t, err)

		result, err := s.onQuery(sess, "SELECT name, id FROM rc_cols;")

		// THEN
		require.NoError(t, err)
		require.Len(t, result.columns, 2)
		assert.Equal(t, "name", result.columns[0].name)
		assert.Equal(t, "id", result.columns[1].name)
	})

	t.Run("テーブル名なしのカラム指定にテーブル名が補完される", func(t *testing.T) {
		// GIVEN
		setupTestServer(t)
		defer handler.Reset()
		s := &Server{}
		sess := newSession("", 0)

		_, err := s.onQuery(sess, "CREATE TABLE rc_tbl (id VARCHAR, PRIMARY KEY (id));")
		require.NoError(t, err)

		result, err := s.onQuery(sess, "SELECT id FROM rc_tbl;")

		// THEN
		require.NoError(t, err)
		require.Len(t, result.columns, 1)
		assert.Equal(t, "rc_tbl", result.columns[0].tableName)
		assert.Equal(t, "id", result.columns[0].name)
	})

	t.Run("JOIN の SELECT * で全テーブルの全カラムが返る", func(t *testing.T) {
		// GIVEN
		setupTestServer(t)
		defer handler.Reset()
		s := &Server{}
		sess := newSession("", 0)

		_, err := s.onQuery(sess, "CREATE TABLE rc_users (id VARCHAR, name VARCHAR, PRIMARY KEY (id));")
		require.NoError(t, err)
		_, err = s.onQuery(sess, "CREATE TABLE rc_orders (id VARCHAR, user_id VARCHAR, PRIMARY KEY (id));")
		require.NoError(t, err)

		result, err := s.onQuery(sess, "SELECT * FROM rc_users INNER JOIN rc_orders ON rc_users.id = rc_orders.user_id;")

		// THEN
		require.NoError(t, err)
		require.Len(t, result.columns, 4)
		assert.Equal(t, "rc_users", result.columns[0].tableName)
		assert.Equal(t, "id", result.columns[0].name)
		assert.Equal(t, "rc_users", result.columns[1].tableName)
		assert.Equal(t, "name", result.columns[1].name)
		assert.Equal(t, "rc_orders", result.columns[2].tableName)
		assert.Equal(t, "id", result.columns[2].name)
		assert.Equal(t, "rc_orders", result.columns[3].tableName)
		assert.Equal(t, "user_id", result.columns[3].name)
	})

	t.Run("JOIN のカラム指定で指定したカラムのみ返る", func(t *testing.T) {
		// GIVEN
		setupTestServer(t)
		defer handler.Reset()
		s := &Server{}
		sess := newSession("", 0)

		_, err := s.onQuery(sess, "CREATE TABLE rc_u2 (id VARCHAR, name VARCHAR, PRIMARY KEY (id));")
		require.NoError(t, err)
		_, err = s.onQuery(sess, "CREATE TABLE rc_o2 (id VARCHAR, user_id VARCHAR, PRIMARY KEY (id));")
		require.NoError(t, err)

		result, err := s.onQuery(sess, "SELECT rc_u2.name, rc_o2.id FROM rc_u2 INNER JOIN rc_o2 ON rc_u2.id = rc_o2.user_id;")

		// THEN
		require.NoError(t, err)
		require.Len(t, result.columns, 2)
		assert.Equal(t, "rc_u2", result.columns[0].tableName)
		assert.Equal(t, "name", result.columns[0].name)
		assert.Equal(t, "rc_o2", result.columns[1].tableName)
		assert.Equal(t, "id", result.columns[1].name)
	})

	t.Run("DDL は resultOK を返す (columns は nil)", func(t *testing.T) {
		// GIVEN
		setupTestServer(t)
		defer handler.Reset()
		s := &Server{}
		sess := newSession("", 0)

		// WHEN
		result, err := s.onQuery(sess, "CREATE TABLE rc_ddl (id VARCHAR, PRIMARY KEY (id));")

		// THEN
		require.NoError(t, err)
		assert.Equal(t, resultOK, result.resultType)
		assert.Nil(t, result.columns)
	})
}

// setupTestServer はテスト用のサーバーを初期化する
func setupTestServer(t *testing.T) *Server {
	t.Helper()
	tmpdir := t.TempDir()
	t.Setenv("MINESQL_DATA_DIR", tmpdir)
	t.Setenv("MINESQL_BUFFER_SIZE", "100")
	handler.Reset()
	handler.Init()

	// テスト用の ACL を構築
	hdl := handler.Get()
	authString, err := acl.CryptPassword("root")
	if err != nil {
		t.Fatalf("failed to crypt password: %v", err)
	}
	hdl.ACL = acl.NewACLFromCatalog("root", "%", authString)

	tlsConfig, err := loadOrGenerateTLSConfig(tmpdir)
	if err != nil {
		t.Fatalf("failed to load TLS config: %v", err)
	}
	return &Server{tlsConfig: tlsConfig}
}

// resultToCSV は queryResult のレコードを CSV 形式の文字列に変換する (テスト用)
func resultToCSV(result *queryResult) string {
	var sb strings.Builder
	for _, record := range result.records {
		line := make([]string, len(record))
		for i, col := range record {
			line[i] = string(col)
		}
		sb.WriteString(strings.Join(line, ",") + "\n")
	}
	return sb.String()
}
