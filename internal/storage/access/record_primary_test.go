package access

import (
	"encoding/binary"
	"testing"

	"github.com/ren-yamanashi/minesql/internal/storage/btree/node"
	"github.com/ren-yamanashi/minesql/internal/storage/encode"
	"github.com/ren-yamanashi/minesql/internal/storage/lock"
	"github.com/ren-yamanashi/minesql/internal/storage/page"
	"github.com/ren-yamanashi/minesql/internal/storage/undo"
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

	t.Run("lastTrxId と rollPtr が設定される", func(t *testing.T) {
		// GIVEN
		ct := setupSecondaryTestCatalog(t)
		rollPtr := undo.Pointer{PageNumber: 3, Offset: 64}

		// WHEN
		pr, err := newPrimaryRecord(ct, newPrimaryRecordInput{
			fileId: page.FileId(2), pkCount: 1, deleteMark: 0,
			lastTrxId: 100, rollPtr: rollPtr,
			colNames: []string{"id", "name", "email"},
			values:   []string{"1", "Alice", "a@b.com"},
		})

		// THEN
		assert.NoError(t, err)
		assert.Equal(t, lock.TrxId(100), pr.lastTrxId)
		assert.Equal(t, rollPtr, pr.rollPtr)
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
		updated, err := pr.update(10, []string{"name"}, []string{"Bob"})

		// THEN
		assert.NoError(t, err)
		assert.Equal(t, []string{"id", "name", "email"}, updated.ColNames)
		assert.Equal(t, []string{"1", "Bob", "alice@example.com"}, updated.Values)
	})

	t.Run("更新後のレコードに新しい trxId が設定され rollPtr は元の値が保持される", func(t *testing.T) {
		// GIVEN
		ct := setupSecondaryTestCatalog(t)
		rollPtr := undo.Pointer{PageNumber: 5, Offset: 128}
		pr, _ := newPrimaryRecord(ct, newPrimaryRecordInput{
			fileId: page.FileId(2), pkCount: 1, deleteMark: 0,
			lastTrxId: 10, rollPtr: rollPtr,
			colNames: []string{"id", "name", "email"},
			values:   []string{"1", "Alice", "a@b.com"},
		})

		// WHEN
		updated, err := pr.update(42, []string{"name"}, []string{"Bob"})

		// THEN
		assert.NoError(t, err)
		assert.Equal(t, lock.TrxId(42), updated.lastTrxId)
		assert.Equal(t, rollPtr, updated.rollPtr)
	})

	t.Run("元のレコードは変更されない", func(t *testing.T) {
		// GIVEN
		ct := setupSecondaryTestCatalog(t)
		pr, _ := newPrimaryRecord(ct, newPrimaryRecordInput{fileId: page.FileId(2), pkCount: 1, deleteMark: 0, colNames: []string{"id", "name", "email"}, values: []string{"1", "Alice", "alice@example.com"}})

		// WHEN
		_, err := pr.update(10, []string{"name"}, []string{"Bob"})

		// THEN
		assert.NoError(t, err)
		assert.Equal(t, []string{"1", "Alice", "alice@example.com"}, pr.Values)
	})

	t.Run("複数カラムを同時に更新できる", func(t *testing.T) {
		// GIVEN
		ct := setupSecondaryTestCatalog(t)
		pr, _ := newPrimaryRecord(ct, newPrimaryRecordInput{fileId: page.FileId(2), pkCount: 1, deleteMark: 0, colNames: []string{"id", "name", "email"}, values: []string{"1", "Alice", "alice@example.com"}})

		// WHEN
		updated, err := pr.update(10, []string{"name", "email"}, []string{"Bob", "bob@example.com"})

		// THEN
		assert.NoError(t, err)
		assert.Equal(t, []string{"1", "Bob", "bob@example.com"}, updated.Values)
	})

	t.Run("カラム名と値の数が一致しない場合エラーを返す", func(t *testing.T) {
		// GIVEN
		ct := setupSecondaryTestCatalog(t)
		pr, _ := newPrimaryRecord(ct, newPrimaryRecordInput{fileId: page.FileId(2), pkCount: 1, deleteMark: 0, colNames: []string{"id", "name", "email"}, values: []string{"1", "Alice", "a@b.com"}})

		// WHEN
		_, err := pr.update(10, []string{"name", "email"}, []string{"Bob"})

		// THEN
		assert.Error(t, err)
	})

	t.Run("存在しないカラム名を指定するとエラーを返す", func(t *testing.T) {
		// GIVEN
		ct := setupSecondaryTestCatalog(t)
		pr, _ := newPrimaryRecord(ct, newPrimaryRecordInput{fileId: page.FileId(2), pkCount: 1, deleteMark: 0, colNames: []string{"id", "name", "email"}, values: []string{"1", "Alice", "a@b.com"}})

		// WHEN
		_, err := pr.update(10, []string{"nonexistent"}, []string{"val"})

		// THEN
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "not found in record")
	})

	t.Run("重複カラム名を指定するとエラーを返す", func(t *testing.T) {
		// GIVEN
		ct := setupSecondaryTestCatalog(t)
		pr, _ := newPrimaryRecord(ct, newPrimaryRecordInput{fileId: page.FileId(2), pkCount: 1, deleteMark: 0, colNames: []string{"id", "name", "email"}, values: []string{"1", "Alice", "a@b.com"}})

		// WHEN
		_, err := pr.update(10, []string{"name", "name"}, []string{"Bob", "Charlie"})

		// THEN
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "duplicate column")
	})
}

func TestPrimaryRecordSetRollPtr(t *testing.T) {
	t.Run("rollPtr を設定できる", func(t *testing.T) {
		// GIVEN
		ct := setupSecondaryTestCatalog(t)
		pr, _ := newPrimaryRecord(ct, newPrimaryRecordInput{
			fileId: page.FileId(2), pkCount: 1, deleteMark: 0,
			colNames: []string{"id", "name", "email"},
			values:   []string{"1", "Alice", "a@b.com"},
		})
		assert.Equal(t, undo.Pointer{}, pr.rollPtr)

		// WHEN
		newPtr := undo.Pointer{PageNumber: 7, Offset: 256}
		pr.setRollPtr(newPtr)

		// THEN
		assert.Equal(t, newPtr, pr.rollPtr)
	})

	t.Run("NullPointer を設定できる", func(t *testing.T) {
		// GIVEN
		ct := setupSecondaryTestCatalog(t)
		pr, _ := newPrimaryRecord(ct, newPrimaryRecordInput{
			fileId: page.FileId(2), pkCount: 1, deleteMark: 0,
			rollPtr:  undo.Pointer{PageNumber: 1, Offset: 10},
			colNames: []string{"id", "name", "email"},
			values:   []string{"1", "Alice", "a@b.com"},
		})

		// WHEN
		pr.setRollPtr(undo.NullPointer)

		// THEN
		assert.Equal(t, undo.NullPointer, pr.rollPtr)
	})

	t.Run("setRollPtr 後の Encode に反映される", func(t *testing.T) {
		// GIVEN
		ct := setupSecondaryTestCatalog(t)
		pr, _ := newPrimaryRecord(ct, newPrimaryRecordInput{
			fileId: page.FileId(2), pkCount: 1, deleteMark: 0,
			colNames: []string{"id", "name", "email"},
			values:   []string{"1", "Alice", "a@b.com"},
		})
		newPtr := undo.Pointer{PageNumber: 3, Offset: 64}
		pr.setRollPtr(newPtr)

		// WHEN
		record := pr.Encode()

		// THEN
		nonKey := record.NonKey()
		assert.Equal(t, newPtr.Encode(), nonKey[lock.TrxIdSize:lock.TrxIdSize+undo.PointerSize])
	})
}

func TestPrimaryRecordEncode(t *testing.T) {
	t.Run("プライマリキーと非キーカラムをエンコードしたレコードを返す", func(t *testing.T) {
		// GIVEN
		ct := setupSecondaryTestCatalog(t)
		pr, _ := newPrimaryRecord(ct, newPrimaryRecordInput{fileId: page.FileId(2), pkCount: 1, deleteMark: 0, colNames: []string{"id", "name", "email"}, values: []string{"1", "Alice", "alice@example.com"}})

		// WHEN
		record := pr.Encode()

		// THEN
		assert.Equal(t, []byte{0x00}, record.Header())

		var decodedKey [][]byte
		encode.Decode(record.Key(), &decodedKey)
		assert.Equal(t, [][]byte{[]byte("1")}, decodedKey)

		// 非キー領域: lastTrxId (4B) + rollPtr (4B) + カラムデータ
		nonKey := record.NonKey()
		assert.True(t, len(nonKey) >= lock.TrxIdSize+undo.PointerSize)
		var decodedNonKey [][]byte
		encode.Decode(nonKey[lock.TrxIdSize+undo.PointerSize:], &decodedNonKey)
		assert.Equal(t, [][]byte{[]byte("Alice"), []byte("alice@example.com")}, decodedNonKey)
	})

	t.Run("非キー領域の先頭に lastTrxId と rollPtr がエンコードされる", func(t *testing.T) {
		// GIVEN
		ct := setupSecondaryTestCatalog(t)
		rollPtr := undo.Pointer{PageNumber: 3, Offset: 64}
		pr, _ := newPrimaryRecord(ct, newPrimaryRecordInput{
			fileId: page.FileId(2), pkCount: 1, deleteMark: 0,
			lastTrxId: 100, rollPtr: rollPtr,
			colNames: []string{"id", "name", "email"},
			values:   []string{"1", "Alice", "a@b.com"},
		})

		// WHEN
		record := pr.Encode()

		// THEN
		nonKey := record.NonKey()
		assert.Equal(t, uint32(100), binary.BigEndian.Uint32(nonKey[:lock.TrxIdSize]))
		assert.Equal(t, rollPtr.Encode(), nonKey[lock.TrxIdSize:lock.TrxIdSize+undo.PointerSize])
	})

	t.Run("複合プライマリキーを正しくエンコードする", func(t *testing.T) {
		// GIVEN
		ct := setupSecondaryTestCatalog(t)
		pr, _ := newPrimaryRecord(ct, newPrimaryRecordInput{fileId: page.FileId(2), pkCount: 2, deleteMark: 0, colNames: []string{"id", "name", "email"}, values: []string{"1", "Alice", "alice@example.com"}})

		// WHEN
		record := pr.Encode()

		// THEN
		var decodedKey [][]byte
		encode.Decode(record.Key(), &decodedKey)
		assert.Equal(t, [][]byte{[]byte("1"), []byte("Alice")}, decodedKey)

		nonKey := record.NonKey()
		var decodedNonKey [][]byte
		encode.Decode(nonKey[lock.TrxIdSize+undo.PointerSize:], &decodedNonKey)
		assert.Equal(t, [][]byte{[]byte("alice@example.com")}, decodedNonKey)
	})

	t.Run("削除マークがヘッダーに設定される", func(t *testing.T) {
		// GIVEN
		ct := setupSecondaryTestCatalog(t)
		pr, _ := newPrimaryRecord(ct, newPrimaryRecordInput{fileId: page.FileId(2), pkCount: 1, deleteMark: 1, colNames: []string{"id", "name", "email"}, values: []string{"1", "Alice", "a@b.com"}})

		// WHEN
		record := pr.Encode()

		// THEN
		assert.Equal(t, []byte{0x01}, record.Header())
	})
}

func TestDecodePrimaryRecord(t *testing.T) {
	t.Run("エンコードしたレコードをデコードすると元のデータに戻る", func(t *testing.T) {
		// GIVEN
		ct := setupSecondaryTestCatalog(t)
		original, _ := newPrimaryRecord(ct, newPrimaryRecordInput{fileId: page.FileId(2), pkCount: 1, deleteMark: 0, colNames: []string{"id", "name", "email"}, values: []string{"1", "Alice", "alice@example.com"}})
		encoded := original.Encode()

		// WHEN
		decoded, err := decodePrimaryRecord(encoded, ct, page.FileId(2))

		// THEN
		assert.NoError(t, err)
		assert.Equal(t, original.ColNames, decoded.ColNames)
		assert.Equal(t, original.Values, decoded.Values)
		assert.Equal(t, original.pkCount, decoded.pkCount)
		assert.Equal(t, original.deleteMark, decoded.deleteMark)
	})

	t.Run("lastTrxId と rollPtr がデコードされる", func(t *testing.T) {
		// GIVEN
		ct := setupSecondaryTestCatalog(t)
		rollPtr := undo.Pointer{PageNumber: 5, Offset: 128}
		original, _ := newPrimaryRecord(ct, newPrimaryRecordInput{
			fileId: page.FileId(2), pkCount: 1, deleteMark: 0,
			lastTrxId: 42, rollPtr: rollPtr,
			colNames: []string{"id", "name", "email"},
			values:   []string{"1", "Alice", "a@b.com"},
		})
		encoded := original.Encode()

		// WHEN
		decoded, err := decodePrimaryRecord(encoded, ct, page.FileId(2))

		// THEN
		assert.NoError(t, err)
		assert.Equal(t, lock.TrxId(42), decoded.lastTrxId)
		assert.Equal(t, rollPtr, decoded.rollPtr)
	})

	t.Run("NullPointer のラウンドトリップ", func(t *testing.T) {
		// GIVEN
		ct := setupSecondaryTestCatalog(t)
		original, _ := newPrimaryRecord(ct, newPrimaryRecordInput{
			fileId: page.FileId(2), pkCount: 1, deleteMark: 0,
			lastTrxId: 0, rollPtr: undo.NullPointer,
			colNames: []string{"id", "name", "email"},
			values:   []string{"1", "Alice", "a@b.com"},
		})
		encoded := original.Encode()

		// WHEN
		decoded, err := decodePrimaryRecord(encoded, ct, page.FileId(2))

		// THEN
		assert.NoError(t, err)
		assert.Equal(t, undo.NullPointer, decoded.rollPtr)
	})

	t.Run("削除マーク付きレコードをデコードできる", func(t *testing.T) {
		// GIVEN
		ct := setupSecondaryTestCatalog(t)
		original, _ := newPrimaryRecord(ct, newPrimaryRecordInput{fileId: page.FileId(2), pkCount: 1, deleteMark: 1, colNames: []string{"id", "name", "email"}, values: []string{"1", "Alice", "a@b.com"}})
		encoded := original.Encode()

		// WHEN
		decoded, err := decodePrimaryRecord(encoded, ct, page.FileId(2))

		// THEN
		assert.NoError(t, err)
		assert.Equal(t, byte(1), decoded.deleteMark)
	})

	t.Run("非キー領域が短すぎる場合エラーを返す", func(t *testing.T) {
		// GIVEN
		ct := setupSecondaryTestCatalog(t)
		var key []byte
		encode.Encode([][]byte{[]byte("1")}, &key)
		// 非キー領域が空 (lastTrxId + rollPtr の 8B に満たない)
		record := node.NewRecord([]byte{0x00}, key, nil)

		// WHEN
		_, err := decodePrimaryRecord(record, ct, page.FileId(2))

		// THEN
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "non-key data too short")
	})

	t.Run("カラム数が不一致の場合エラーを返す", func(t *testing.T) {
		// GIVEN
		ct := setupSecondaryTestCatalog(t)
		var key []byte
		encode.Encode([][]byte{[]byte("1")}, &key)
		// lastTrxId + rollPtr だけでカラムデータなし → pkCount=1, カラム合計=1 (テーブル定義は 3)
		nonKey := make([]byte, lock.TrxIdSize+undo.PointerSize)

		record := node.NewRecord([]byte{0x00}, key, nonKey)

		// WHEN
		_, err := decodePrimaryRecord(record, ct, page.FileId(2))

		// THEN
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "column count mismatch")
	})
}
