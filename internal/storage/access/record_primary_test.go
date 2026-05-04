package access

import (
	"testing"

	"github.com/ren-yamanashi/minesql/internal/storage/btree/node"
	"github.com/ren-yamanashi/minesql/internal/storage/encode"
	"github.com/ren-yamanashi/minesql/internal/storage/page"
	"github.com/stretchr/testify/assert"
)

func TestNewPrimaryRecord(t *testing.T) {
	t.Run("カタログを参照してテーブル定義順に並び替えたレコードを返す", func(t *testing.T) {
		// GIVEN
		ct := setupSecondaryTestCatalog(t) // id:0, name:1, email:2

		// WHEN
		pr, err := newPrimaryRecord(ct, newPrimaryRecordInput{fileId: page.FileId(2), pkCount: 1, deleteMark: 0, colNames: []string{"email", "name", "id"}, values: []string{"alice@example.com", "Alice", "1"}})

		// THEN
		assert.NoError(t, err)
		assert.Equal(t, []string{"id", "name", "email"}, pr.ColNames)
		assert.Equal(t, []string{"1", "Alice", "alice@example.com"}, pr.Values)
		assert.Equal(t, 1, pr.pkCount)
		assert.Equal(t, byte(0), pr.deleteMark)
	})

	t.Run("カラム名と値の数が一致しない場合エラーを返す", func(t *testing.T) {
		// GIVEN
		ct := setupSecondaryTestCatalog(t)

		// WHEN
		_, err := newPrimaryRecord(ct, newPrimaryRecordInput{fileId: page.FileId(2), pkCount: 1, deleteMark: 0, colNames: []string{"id", "name"}, values: []string{"1"}})

		// THEN
		assert.Error(t, err)
	})

	t.Run("カラム数がテーブル定義と一致しない場合エラーを返す", func(t *testing.T) {
		// GIVEN
		ct := setupSecondaryTestCatalog(t)

		// WHEN
		_, err := newPrimaryRecord(ct, newPrimaryRecordInput{fileId: page.FileId(2), pkCount: 1, deleteMark: 0, colNames: []string{"id"}, values: []string{"1"}})

		// THEN
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "column count mismatch")
	})

	t.Run("存在しないカラム名を指定するとエラーを返す", func(t *testing.T) {
		// GIVEN
		ct := setupSecondaryTestCatalog(t)

		// WHEN
		_, err := newPrimaryRecord(ct, newPrimaryRecordInput{fileId: page.FileId(2), pkCount: 1, deleteMark: 0, colNames: []string{"id", "name", "nonexistent"}, values: []string{"1", "Alice", "x"}})

		// THEN
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "not found in table definition")
	})

	t.Run("重複カラム名を指定するとエラーを返す", func(t *testing.T) {
		// GIVEN
		ct := setupSecondaryTestCatalog(t)

		// WHEN
		_, err := newPrimaryRecord(ct, newPrimaryRecordInput{fileId: page.FileId(2), pkCount: 1, deleteMark: 0, colNames: []string{"id", "id", "name"}, values: []string{"1", "2", "Alice"}})

		// THEN
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "duplicate column")
	})

	t.Run("削除マークが設定される", func(t *testing.T) {
		// GIVEN
		ct := setupSecondaryTestCatalog(t)

		// WHEN
		pr, err := newPrimaryRecord(ct, newPrimaryRecordInput{fileId: page.FileId(2), pkCount: 1, deleteMark: 1, colNames: []string{"id", "name", "email"}, values: []string{"1", "Alice", "a@b.com"}})

		// THEN
		assert.NoError(t, err)
		assert.Equal(t, byte(1), pr.deleteMark)
	})
}

func TestPrimaryRecordUpdate(t *testing.T) {
	t.Run("指定したカラムの値だけ更新した新しいレコードを返す", func(t *testing.T) {
		// GIVEN
		ct := setupSecondaryTestCatalog(t)
		pr, _ := newPrimaryRecord(ct, newPrimaryRecordInput{fileId: page.FileId(2), pkCount: 1, deleteMark: 0, colNames: []string{"id", "name", "email"}, values: []string{"1", "Alice", "alice@example.com"}})

		// WHEN
		updated, err := pr.update([]string{"name"}, []string{"Bob"})

		// THEN
		assert.NoError(t, err)
		assert.Equal(t, []string{"id", "name", "email"}, updated.ColNames)
		assert.Equal(t, []string{"1", "Bob", "alice@example.com"}, updated.Values)
	})

	t.Run("元のレコードは変更されない", func(t *testing.T) {
		// GIVEN
		ct := setupSecondaryTestCatalog(t)
		pr, _ := newPrimaryRecord(ct, newPrimaryRecordInput{fileId: page.FileId(2), pkCount: 1, deleteMark: 0, colNames: []string{"id", "name", "email"}, values: []string{"1", "Alice", "alice@example.com"}})

		// WHEN
		_, err := pr.update([]string{"name"}, []string{"Bob"})

		// THEN
		assert.NoError(t, err)
		assert.Equal(t, []string{"1", "Alice", "alice@example.com"}, pr.Values)
	})

	t.Run("複数カラムを同時に更新できる", func(t *testing.T) {
		// GIVEN
		ct := setupSecondaryTestCatalog(t)
		pr, _ := newPrimaryRecord(ct, newPrimaryRecordInput{fileId: page.FileId(2), pkCount: 1, deleteMark: 0, colNames: []string{"id", "name", "email"}, values: []string{"1", "Alice", "alice@example.com"}})

		// WHEN
		updated, err := pr.update([]string{"name", "email"}, []string{"Bob", "bob@example.com"})

		// THEN
		assert.NoError(t, err)
		assert.Equal(t, []string{"1", "Bob", "bob@example.com"}, updated.Values)
	})

	t.Run("カラム名と値の数が一致しない場合エラーを返す", func(t *testing.T) {
		// GIVEN
		ct := setupSecondaryTestCatalog(t)
		pr, _ := newPrimaryRecord(ct, newPrimaryRecordInput{fileId: page.FileId(2), pkCount: 1, deleteMark: 0, colNames: []string{"id", "name", "email"}, values: []string{"1", "Alice", "a@b.com"}})

		// WHEN
		_, err := pr.update([]string{"name", "email"}, []string{"Bob"})

		// THEN
		assert.Error(t, err)
	})

	t.Run("存在しないカラム名を指定するとエラーを返す", func(t *testing.T) {
		// GIVEN
		ct := setupSecondaryTestCatalog(t)
		pr, _ := newPrimaryRecord(ct, newPrimaryRecordInput{fileId: page.FileId(2), pkCount: 1, deleteMark: 0, colNames: []string{"id", "name", "email"}, values: []string{"1", "Alice", "a@b.com"}})

		// WHEN
		_, err := pr.update([]string{"nonexistent"}, []string{"val"})

		// THEN
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "not found in record")
	})

	t.Run("重複カラム名を指定するとエラーを返す", func(t *testing.T) {
		// GIVEN
		ct := setupSecondaryTestCatalog(t)
		pr, _ := newPrimaryRecord(ct, newPrimaryRecordInput{fileId: page.FileId(2), pkCount: 1, deleteMark: 0, colNames: []string{"id", "name", "email"}, values: []string{"1", "Alice", "a@b.com"}})

		// WHEN
		_, err := pr.update([]string{"name", "name"}, []string{"Bob", "Charlie"})

		// THEN
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "duplicate column")
	})
}

func TestPrimaryRecordEncode(t *testing.T) {
	t.Run("プライマリキーと非キーカラムをエンコードしたレコードを返す", func(t *testing.T) {
		// GIVEN
		ct := setupSecondaryTestCatalog(t)
		pr, _ := newPrimaryRecord(ct, newPrimaryRecordInput{fileId: page.FileId(2), pkCount: 1, deleteMark: 0, colNames: []string{"id", "name", "email"}, values: []string{"1", "Alice", "alice@example.com"}})

		// WHEN
		record := pr.encode()

		// THEN
		assert.Equal(t, []byte{0x00}, record.Header())

		var decodedKey [][]byte
		encode.Decode(record.Key(), &decodedKey)
		assert.Equal(t, [][]byte{[]byte("1")}, decodedKey)

		var decodedNonKey [][]byte
		encode.Decode(record.NonKey(), &decodedNonKey)
		assert.Equal(t, [][]byte{[]byte("Alice"), []byte("alice@example.com")}, decodedNonKey)
	})

	t.Run("複合プライマリキーを正しくエンコードする", func(t *testing.T) {
		// GIVEN
		ct := setupSecondaryTestCatalog(t)
		pr, _ := newPrimaryRecord(ct, newPrimaryRecordInput{fileId: page.FileId(2), pkCount: 2, deleteMark: 0, colNames: []string{"id", "name", "email"}, values: []string{"1", "Alice", "alice@example.com"}})

		// WHEN
		record := pr.encode()

		// THEN
		var decodedKey [][]byte
		encode.Decode(record.Key(), &decodedKey)
		assert.Equal(t, [][]byte{[]byte("1"), []byte("Alice")}, decodedKey)

		var decodedNonKey [][]byte
		encode.Decode(record.NonKey(), &decodedNonKey)
		assert.Equal(t, [][]byte{[]byte("alice@example.com")}, decodedNonKey)
	})

	t.Run("削除マークがヘッダーに設定される", func(t *testing.T) {
		// GIVEN
		ct := setupSecondaryTestCatalog(t)
		pr, _ := newPrimaryRecord(ct, newPrimaryRecordInput{fileId: page.FileId(2), pkCount: 1, deleteMark: 1, colNames: []string{"id", "name", "email"}, values: []string{"1", "Alice", "a@b.com"}})

		// WHEN
		record := pr.encode()

		// THEN
		assert.Equal(t, []byte{0x01}, record.Header())
	})
}

func TestDecodePrimaryRecord(t *testing.T) {
	t.Run("エンコードしたレコードをデコードすると元のデータに戻る", func(t *testing.T) {
		// GIVEN
		ct := setupSecondaryTestCatalog(t)
		original, _ := newPrimaryRecord(ct, newPrimaryRecordInput{fileId: page.FileId(2), pkCount: 1, deleteMark: 0, colNames: []string{"id", "name", "email"}, values: []string{"1", "Alice", "alice@example.com"}})
		encoded := original.encode()

		// WHEN
		decoded, err := decodePrimaryRecord(encoded, ct, page.FileId(2))

		// THEN
		assert.NoError(t, err)
		assert.Equal(t, original.ColNames, decoded.ColNames)
		assert.Equal(t, original.Values, decoded.Values)
		assert.Equal(t, original.pkCount, decoded.pkCount)
		assert.Equal(t, original.deleteMark, decoded.deleteMark)
	})

	t.Run("削除マーク付きレコードをデコードできる", func(t *testing.T) {
		// GIVEN
		ct := setupSecondaryTestCatalog(t)
		original, _ := newPrimaryRecord(ct, newPrimaryRecordInput{fileId: page.FileId(2), pkCount: 1, deleteMark: 1, colNames: []string{"id", "name", "email"}, values: []string{"1", "Alice", "a@b.com"}})
		encoded := original.encode()

		// WHEN
		decoded, err := decodePrimaryRecord(encoded, ct, page.FileId(2))

		// THEN
		assert.NoError(t, err)
		assert.Equal(t, byte(1), decoded.deleteMark)
	})

	t.Run("カラム数が不一致の場合エラーを返す", func(t *testing.T) {
		// GIVEN
		ct := setupSecondaryTestCatalog(t)
		// カラム 1 つだけの不正なレコード
		var key []byte
		encode.Encode([][]byte{[]byte("1")}, &key)
		record := node.NewRecord([]byte{0x00}, key, nil)

		// WHEN
		_, err := decodePrimaryRecord(record, ct, page.FileId(2))

		// THEN
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "column count mismatch")
	})
}
