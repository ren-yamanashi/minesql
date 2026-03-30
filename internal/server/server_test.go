package server

import (
	"minesql/internal/engine"
	"minesql/internal/storage/transaction"
	"minesql/internal/storage/undo"
	"testing"

	"github.com/stretchr/testify/assert"
)

func setupTestServer(t *testing.T) *Server {
	t.Helper()
	tmpdir := t.TempDir()
	t.Setenv("MINESQL_DATA_DIR", tmpdir)
	t.Setenv("MINESQL_BUFFER_SIZE", "100")
	engine.Reset()
	engine.Init()

	undoLog := undo.NewUndoLog()
	return &Server{
		undoLog:    undoLog,
		trxManager: transaction.NewManager(undoLog),
	}
}

func TestNewServer(t *testing.T) {
	t.Run("Server が正しく初期化される", func(t *testing.T) {
		// WHEN
		s := NewServer("localhost", 3307)

		// THEN
		assert.Equal(t, "localhost", s.Address)
		assert.Equal(t, 3307, s.Port)
		assert.NotNil(t, s.undoLog)
		assert.NotNil(t, s.trxManager)
	})
}

func TestNewSession(t *testing.T) {
	t.Run("session が初期状態で生成される", func(t *testing.T) {
		// WHEN
		sess := newSession()

		// THEN
		assert.NotNil(t, sess)
		assert.Equal(t, undo.TrxId(0), sess.trxId)
	})
}

func TestExecuteQuery(t *testing.T) {
	t.Run("CREATE TABLE を実行できる", func(t *testing.T) {
		// GIVEN
		s := setupTestServer(t)
		defer engine.Reset()
		sess := newSession()

		// WHEN
		result, err := s.executeQuery(sess, "CREATE TABLE users (id VARCHAR, name VARCHAR, PRIMARY KEY (id));")

		// THEN
		assert.NoError(t, err)
		assert.Equal(t, "", result)
	})

	t.Run("INSERT と SELECT を実行できる", func(t *testing.T) {
		// GIVEN
		s := setupTestServer(t)
		defer engine.Reset()
		sess := newSession()

		_, err := s.executeQuery(sess, "CREATE TABLE users (id VARCHAR, name VARCHAR, PRIMARY KEY (id));")
		assert.NoError(t, err)

		// WHEN
		_, err = s.executeQuery(sess, "INSERT INTO users (id, name) VALUES ('1', 'Alice'), ('2', 'Bob');")
		assert.NoError(t, err)

		result, err := s.executeQuery(sess, "SELECT * FROM users;")

		// THEN
		assert.NoError(t, err)
		assert.Contains(t, result, "1,Alice")
		assert.Contains(t, result, "2,Bob")
	})

	t.Run("UPDATE を実行できる", func(t *testing.T) {
		// GIVEN
		s := setupTestServer(t)
		defer engine.Reset()
		sess := newSession()

		_, err := s.executeQuery(sess, "CREATE TABLE users (id VARCHAR, name VARCHAR, PRIMARY KEY (id));")
		assert.NoError(t, err)
		_, err = s.executeQuery(sess, "INSERT INTO users (id, name) VALUES ('1', 'Alice');")
		assert.NoError(t, err)

		// WHEN
		_, err = s.executeQuery(sess, "UPDATE users SET name = 'Carol' WHERE id = '1';")
		assert.NoError(t, err)

		result, err := s.executeQuery(sess, "SELECT * FROM users;")

		// THEN
		assert.NoError(t, err)
		assert.Contains(t, result, "1,Carol")
	})

	t.Run("DELETE を実行できる", func(t *testing.T) {
		// GIVEN
		s := setupTestServer(t)
		defer engine.Reset()
		sess := newSession()

		_, err := s.executeQuery(sess, "CREATE TABLE users (id VARCHAR, name VARCHAR, PRIMARY KEY (id));")
		assert.NoError(t, err)
		_, err = s.executeQuery(sess, "INSERT INTO users (id, name) VALUES ('1', 'Alice'), ('2', 'Bob');")
		assert.NoError(t, err)

		// WHEN
		_, err = s.executeQuery(sess, "DELETE FROM users WHERE id = '1';")
		assert.NoError(t, err)

		result, err := s.executeQuery(sess, "SELECT * FROM users;")

		// THEN
		assert.NoError(t, err)
		assert.NotContains(t, result, "Alice")
		assert.Contains(t, result, "2,Bob")
	})

	t.Run("BEGIN なしの DML は autocommit される", func(t *testing.T) {
		// GIVEN
		s := setupTestServer(t)
		defer engine.Reset()
		sess := newSession()

		_, err := s.executeQuery(sess, "CREATE TABLE users (id VARCHAR, name VARCHAR, PRIMARY KEY (id));")
		assert.NoError(t, err)

		// WHEN: BEGIN なしで INSERT
		_, err = s.executeQuery(sess, "INSERT INTO users (id, name) VALUES ('1', 'Alice');")
		assert.NoError(t, err)

		// THEN: trxId は 0 のまま (autocommit 済み)
		assert.Equal(t, undo.TrxId(0), sess.trxId)

		// THEN: Undo ログが残っていない (Commit で破棄済み)
		// データは永続化されている
		result, err := s.executeQuery(sess, "SELECT * FROM users;")
		assert.NoError(t, err)
		assert.Contains(t, result, "1,Alice")
	})

	t.Run("autocommit のデータは ROLLBACK で取り消せない", func(t *testing.T) {
		// GIVEN: BEGIN なしで INSERT (autocommit)
		s := setupTestServer(t)
		defer engine.Reset()
		sess := newSession()

		_, err := s.executeQuery(sess, "CREATE TABLE users (id VARCHAR, name VARCHAR, PRIMARY KEY (id));")
		assert.NoError(t, err)
		_, err = s.executeQuery(sess, "INSERT INTO users (id, name) VALUES ('1', 'Alice');")
		assert.NoError(t, err)

		// WHEN: その後 BEGIN → ROLLBACK しても autocommit 済みのデータは残る
		_, err = s.executeQuery(sess, "BEGIN;")
		assert.NoError(t, err)
		_, err = s.executeQuery(sess, "INSERT INTO users (id, name) VALUES ('2', 'Bob');")
		assert.NoError(t, err)
		_, err = s.executeQuery(sess, "ROLLBACK;")
		assert.NoError(t, err)

		// THEN: autocommit の Alice は残り、トランザクション内の Bob は消える
		result, err := s.executeQuery(sess, "SELECT * FROM users;")
		assert.NoError(t, err)
		assert.Contains(t, result, "1,Alice")
		assert.NotContains(t, result, "Bob")
	})

	t.Run("不正な SQL はエラーを返す", func(t *testing.T) {
		// GIVEN
		s := setupTestServer(t)
		defer engine.Reset()
		sess := newSession()

		// WHEN
		_, err := s.executeQuery(sess, "INVALID SQL;")

		// THEN
		assert.Error(t, err)
	})
}

func TestExecuteQueryTransaction(t *testing.T) {
	t.Run("BEGIN で trxId が設定される", func(t *testing.T) {
		// GIVEN
		s := setupTestServer(t)
		defer engine.Reset()
		sess := newSession()
		assert.Equal(t, undo.TrxId(0), sess.trxId)

		// WHEN
		result, err := s.executeQuery(sess, "BEGIN;")

		// THEN
		assert.NoError(t, err)
		assert.Equal(t, "", result)
		assert.NotEqual(t, undo.TrxId(0), sess.trxId)
	})

	t.Run("COMMIT で Undo ログが破棄され trxId がリセットされる", func(t *testing.T) {
		// GIVEN
		s := setupTestServer(t)
		defer engine.Reset()
		sess := newSession()

		_, err := s.executeQuery(sess, "BEGIN;")
		assert.NoError(t, err)
		committedTrxId := sess.trxId

		_, err = s.executeQuery(sess, "CREATE TABLE users (id VARCHAR, name VARCHAR, PRIMARY KEY (id));")
		assert.NoError(t, err)

		_, err = s.executeQuery(sess, "INSERT INTO users (id, name) VALUES ('1', 'Alice');")
		assert.NoError(t, err)
		assert.Equal(t, 1, len(s.undoLog.GetRecords(committedTrxId)))

		// WHEN
		result, err := s.executeQuery(sess, "COMMIT;")

		// THEN
		assert.NoError(t, err)
		assert.Equal(t, "", result)
		assert.Nil(t, s.undoLog.GetRecords(committedTrxId))
		assert.Equal(t, undo.TrxId(0), sess.trxId)
	})

	t.Run("ROLLBACK で INSERT が取り消される", func(t *testing.T) {
		// GIVEN
		s := setupTestServer(t)
		defer engine.Reset()
		sess := newSession()

		_, err := s.executeQuery(sess, "CREATE TABLE users (id VARCHAR, name VARCHAR, PRIMARY KEY (id));")
		assert.NoError(t, err)

		_, err = s.executeQuery(sess, "BEGIN;")
		assert.NoError(t, err)

		_, err = s.executeQuery(sess, "INSERT INTO users (id, name) VALUES ('1', 'Alice');")
		assert.NoError(t, err)

		// WHEN
		_, err = s.executeQuery(sess, "ROLLBACK;")
		assert.NoError(t, err)

		// THEN: INSERT が取り消されてテーブルが空
		result, err := s.executeQuery(sess, "SELECT * FROM users;")
		assert.NoError(t, err)
		assert.Equal(t, "", result)
	})

	t.Run("ROLLBACK で UPDATE が取り消される", func(t *testing.T) {
		// GIVEN
		s := setupTestServer(t)
		defer engine.Reset()
		sess := newSession()

		_, err := s.executeQuery(sess, "CREATE TABLE users (id VARCHAR, name VARCHAR, PRIMARY KEY (id));")
		assert.NoError(t, err)
		_, err = s.executeQuery(sess, "BEGIN;")
		assert.NoError(t, err)
		_, err = s.executeQuery(sess, "INSERT INTO users (id, name) VALUES ('1', 'Alice');")
		assert.NoError(t, err)
		_, err = s.executeQuery(sess, "COMMIT;")
		assert.NoError(t, err)

		_, err = s.executeQuery(sess, "BEGIN;")
		assert.NoError(t, err)
		_, err = s.executeQuery(sess, "UPDATE users SET name = 'Carol' WHERE id = '1';")
		assert.NoError(t, err)

		// WHEN
		_, err = s.executeQuery(sess, "ROLLBACK;")
		assert.NoError(t, err)

		// THEN: UPDATE が取り消されて元の値
		result, err := s.executeQuery(sess, "SELECT * FROM users;")
		assert.NoError(t, err)
		assert.Contains(t, result, "1,Alice")
	})

	t.Run("ROLLBACK で DELETE が取り消される", func(t *testing.T) {
		// GIVEN
		s := setupTestServer(t)
		defer engine.Reset()
		sess := newSession()

		_, err := s.executeQuery(sess, "CREATE TABLE users (id VARCHAR, name VARCHAR, PRIMARY KEY (id));")
		assert.NoError(t, err)
		_, err = s.executeQuery(sess, "BEGIN;")
		assert.NoError(t, err)
		_, err = s.executeQuery(sess, "INSERT INTO users (id, name) VALUES ('1', 'Alice');")
		assert.NoError(t, err)
		_, err = s.executeQuery(sess, "COMMIT;")
		assert.NoError(t, err)

		_, err = s.executeQuery(sess, "BEGIN;")
		assert.NoError(t, err)
		_, err = s.executeQuery(sess, "DELETE FROM users WHERE id = '1';")
		assert.NoError(t, err)

		// WHEN
		_, err = s.executeQuery(sess, "ROLLBACK;")
		assert.NoError(t, err)

		// THEN: DELETE が取り消されてレコードが復元
		result, err := s.executeQuery(sess, "SELECT * FROM users;")
		assert.NoError(t, err)
		assert.Contains(t, result, "1,Alice")
	})

	t.Run("COMMIT 後に新しいトランザクションを開始できる", func(t *testing.T) {
		// GIVEN
		s := setupTestServer(t)
		defer engine.Reset()
		sess := newSession()

		_, err := s.executeQuery(sess, "CREATE TABLE users (id VARCHAR, name VARCHAR, PRIMARY KEY (id));")
		assert.NoError(t, err)

		// 1 回目のトランザクション: INSERT → COMMIT
		_, err = s.executeQuery(sess, "BEGIN;")
		assert.NoError(t, err)
		_, err = s.executeQuery(sess, "INSERT INTO users (id, name) VALUES ('1', 'Alice');")
		assert.NoError(t, err)
		_, err = s.executeQuery(sess, "COMMIT;")
		assert.NoError(t, err)

		// 2 回目のトランザクション: INSERT → ROLLBACK
		_, err = s.executeQuery(sess, "BEGIN;")
		assert.NoError(t, err)
		_, err = s.executeQuery(sess, "INSERT INTO users (id, name) VALUES ('2', 'Bob');")
		assert.NoError(t, err)
		_, err = s.executeQuery(sess, "ROLLBACK;")
		assert.NoError(t, err)

		// THEN: 1 回目の INSERT のみ残る
		result, err := s.executeQuery(sess, "SELECT * FROM users;")
		assert.NoError(t, err)
		assert.Contains(t, result, "1,Alice")
		assert.NotContains(t, result, "Bob")
	})

	t.Run("トランザクション中に BEGIN を呼ぶとエラーになる", func(t *testing.T) {
		// GIVEN
		s := setupTestServer(t)
		defer engine.Reset()
		sess := newSession()

		_, err := s.executeQuery(sess, "BEGIN;")
		assert.NoError(t, err)

		// WHEN
		_, err = s.executeQuery(sess, "BEGIN;")

		// THEN
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "transaction already started")
	})

	t.Run("BEGIN なしで COMMIT を呼ぶとエラーになる", func(t *testing.T) {
		// GIVEN
		s := setupTestServer(t)
		defer engine.Reset()
		sess := newSession()

		// WHEN
		_, err := s.executeQuery(sess, "COMMIT;")

		// THEN
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "no active transaction")
	})

	t.Run("BEGIN なしで ROLLBACK を呼ぶとエラーになる", func(t *testing.T) {
		// GIVEN
		s := setupTestServer(t)
		defer engine.Reset()
		sess := newSession()

		// WHEN
		_, err := s.executeQuery(sess, "ROLLBACK;")

		// THEN
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "no active transaction")
	})

	t.Run("異なるセッションのトランザクションは独立している", func(t *testing.T) {
		// GIVEN
		s := setupTestServer(t)
		defer engine.Reset()
		sessA := newSession()
		sessB := newSession()

		_, err := s.executeQuery(sessA, "CREATE TABLE users (id VARCHAR, name VARCHAR, PRIMARY KEY (id));")
		assert.NoError(t, err)

		// セッション A で BEGIN + INSERT
		_, err = s.executeQuery(sessA, "BEGIN;")
		assert.NoError(t, err)
		_, err = s.executeQuery(sessA, "INSERT INTO users (id, name) VALUES ('1', 'Alice');")
		assert.NoError(t, err)

		// WHEN: セッション B で BEGIN + ROLLBACK
		_, err = s.executeQuery(sessB, "BEGIN;")
		assert.NoError(t, err)
		_, err = s.executeQuery(sessB, "ROLLBACK;")
		assert.NoError(t, err)

		// THEN: セッション A のデータは影響を受けない
		_, err = s.executeQuery(sessA, "COMMIT;")
		assert.NoError(t, err)

		result, err := s.executeQuery(sessA, "SELECT * FROM users;")
		assert.NoError(t, err)
		assert.Contains(t, result, "1,Alice")
	})

	t.Run("接続切断時にアクティブなトランザクションが自動ロールバックされる", func(t *testing.T) {
		// GIVEN: BEGIN → INSERT したが COMMIT していない
		s := setupTestServer(t)
		defer engine.Reset()
		sess := newSession()

		_, err := s.executeQuery(sess, "CREATE TABLE users (id VARCHAR, name VARCHAR, PRIMARY KEY (id));")
		assert.NoError(t, err)

		_, err = s.executeQuery(sess, "BEGIN;")
		assert.NoError(t, err)

		_, err = s.executeQuery(sess, "INSERT INTO users (id, name) VALUES ('1', 'Alice');")
		assert.NoError(t, err)

		// WHEN: 接続切断をシミュレート (handleConnection の defer と同じロジック)
		assert.NotEqual(t, undo.TrxId(0), sess.trxId)
		err = s.trxManager.Rollback(engine.Get().BufferPool, sess.trxId)
		assert.NoError(t, err)
		sess.trxId = 0

		// THEN: INSERT がロールバックされてテーブルが空
		result, err := s.executeQuery(sess, "SELECT * FROM users;")
		assert.NoError(t, err)
		assert.Equal(t, "", result)
	})

	t.Run("トランザクションなしの接続切断では何も起きない", func(t *testing.T) {
		// GIVEN: BEGIN していない
		s := setupTestServer(t)
		defer engine.Reset()
		sess := newSession()

		_, err := s.executeQuery(sess, "CREATE TABLE users (id VARCHAR, name VARCHAR, PRIMARY KEY (id));")
		assert.NoError(t, err)
		_, err = s.executeQuery(sess, "INSERT INTO users (id, name) VALUES ('1', 'Alice');")
		assert.NoError(t, err)

		// WHEN: 接続切断をシミュレート (trxId == 0 なのでロールバックは走らない)
		assert.Equal(t, undo.TrxId(0), sess.trxId)

		// THEN: データはそのまま残る
		result, err := s.executeQuery(sess, "SELECT * FROM users;")
		assert.NoError(t, err)
		assert.Contains(t, result, "1,Alice")
	})
}
