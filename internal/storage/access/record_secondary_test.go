package access

import (
	"path/filepath"
	"testing"

	"github.com/ren-yamanashi/minesql/internal/storage/btree"
	"github.com/ren-yamanashi/minesql/internal/storage/buffer"
	"github.com/ren-yamanashi/minesql/internal/storage/dictionary"
	"github.com/ren-yamanashi/minesql/internal/storage/encode"
	"github.com/ren-yamanashi/minesql/internal/storage/file"
	"github.com/ren-yamanashi/minesql/internal/storage/page"
	"github.com/stretchr/testify/assert"
)

func TestNewSecondaryRecord(t *testing.T) {
	t.Run("カタログを参照してインデックス定義順に並び替えたレコードを返す", func(t *testing.T) {
		// GIVEN
		ct := setupSecondaryTestCatalog(t)

		// WHEN
		sr, err := NewSecondaryRecord(ct, NewSecondaryRecordInput{
			fileId:     page.FileId(2),
			deleteMark: 0,
			indexName:  "idx_name",
			colNames:   []string{"name"},
			values:     []string{"Alice"},
			pk:         []string{"1"},
		})

		// THEN
		assert.NoError(t, err)
		assert.Equal(t, []string{"name"}, sr.colNames)
		assert.Equal(t, []string{"Alice"}, sr.values)
		assert.Equal(t, []string{"1"}, sr.pk)
		assert.Equal(t, byte(0), sr.deleteMark)
	})

	t.Run("カラム名と値の数が一致しない場合エラーを返す", func(t *testing.T) {
		// GIVEN
		ct := setupSecondaryTestCatalog(t)

		// WHEN
		_, err := NewSecondaryRecord(ct, NewSecondaryRecordInput{
			fileId:    page.FileId(2),
			indexName: "idx_name",
			colNames:  []string{"name", "extra"},
			values:    []string{"Alice"},
			pk:        []string{"1"},
		})

		// THEN
		assert.Error(t, err)
	})

	t.Run("存在しないカラム名を指定するとエラーを返す", func(t *testing.T) {
		// GIVEN
		ct := setupSecondaryTestCatalog(t)

		// WHEN
		_, err := NewSecondaryRecord(ct, NewSecondaryRecordInput{
			fileId:    page.FileId(2),
			indexName: "idx_name",
			colNames:  []string{"nonexistent"},
			values:    []string{"val"},
			pk:        []string{"1"},
		})

		// THEN
		assert.Error(t, err)
	})

	t.Run("重複カラム名を指定するとエラーを返す", func(t *testing.T) {
		// GIVEN
		ct := setupSecondaryTestCatalog(t)

		// WHEN
		_, err := NewSecondaryRecord(ct, NewSecondaryRecordInput{
			fileId:    page.FileId(2),
			indexName: "idx_name_email",
			colNames:  []string{"name", "name"},
			values:    []string{"Alice", "Bob"},
			pk:        []string{"1"},
		})

		// THEN
		assert.Error(t, err)
	})

	t.Run("カラム数がインデックス定義と一致しない場合エラーを返す", func(t *testing.T) {
		// GIVEN
		ct := setupSecondaryTestCatalog(t)

		// WHEN
		_, err := NewSecondaryRecord(ct, NewSecondaryRecordInput{
			fileId:    page.FileId(2),
			indexName: "idx_name",
			colNames:  []string{"name", "email"},
			values:    []string{"Alice", "alice@example.com"},
			pk:        []string{"1"},
		})

		// THEN
		assert.Error(t, err)
	})
}

func TestSecondaryRecordEncode(t *testing.T) {
	t.Run("セカンダリキーとプライマリキーをエンコードしたレコードを返す", func(t *testing.T) {
		// GIVEN
		sr := &SecondaryRecord{
			deleteMark: 0x00,
			values:     []string{"sk1"},
			pk:         []string{"pk1"},
		}

		// WHEN
		record := sr.Encode()

		// THEN
		assert.Equal(t, []byte{0x00}, record.Header())

		decoded, err := encode.Decode(record.Key())
		assert.NoError(t, err)
		assert.Equal(t, [][]byte{[]byte("sk1"), []byte("pk1")}, decoded)

		assert.Nil(t, record.NonKey())
	})

	t.Run("複合セカンダリキーを正しくエンコードする", func(t *testing.T) {
		// GIVEN
		sr := &SecondaryRecord{
			deleteMark: 0x00,
			values:     []string{"sk1", "sk2"},
			pk:         []string{"pk1"},
		}

		// WHEN
		record := sr.Encode()

		// THEN
		decoded, err := encode.Decode(record.Key())
		assert.NoError(t, err)
		assert.Equal(t, [][]byte{[]byte("sk1"), []byte("sk2"), []byte("pk1")}, decoded)
	})

	t.Run("削除マークが設定される", func(t *testing.T) {
		// GIVEN
		sr := &SecondaryRecord{
			deleteMark: 0x01,
			values:     []string{"sk1"},
			pk:         []string{"pk1"},
		}

		// WHEN
		record := sr.Encode()

		// THEN
		assert.Equal(t, []byte{0x01}, record.Header())
	})

	t.Run("複合プライマリキーを正しくエンコードする", func(t *testing.T) {
		// GIVEN
		sr := &SecondaryRecord{
			deleteMark: 0x00,
			values:     []string{"sk1"},
			pk:         []string{"pk1", "pk2"},
		}

		// WHEN
		record := sr.Encode()

		// THEN
		decoded, err := encode.Decode(record.Key())
		assert.NoError(t, err)
		assert.Equal(t, [][]byte{[]byte("sk1"), []byte("pk1"), []byte("pk2")}, decoded)
	})
}

func TestSecondaryRecordEncodedSecondaryKey(t *testing.T) {
	t.Run("エンコード済みのセカンダリキーのみを返す", func(t *testing.T) {
		// GIVEN
		sr := &SecondaryRecord{
			values: []string{"sk1"},
			pk:     []string{"pk1"},
		}

		// WHEN
		result := sr.encodedSecondaryKey()

		// THEN
		expected := encode.Encode(nil, [][]byte{[]byte("sk1")})
		assert.Equal(t, expected, result)
	})

	t.Run("複合セカンダリキーの場合も正しくエンコードする", func(t *testing.T) {
		// GIVEN
		sr := &SecondaryRecord{
			values: []string{"sk1", "sk2"},
			pk:     []string{"pk1"},
		}

		// WHEN
		result := sr.encodedSecondaryKey()

		// THEN
		expected := encode.Encode(nil, [][]byte{[]byte("sk1"), []byte("sk2")})
		assert.Equal(t, expected, result)
	})
}

func TestDecodeSecondaryRecord(t *testing.T) {
	t.Run("エンコードしたレコードをデコードすると元のデータに戻る", func(t *testing.T) {
		// GIVEN
		ct := setupSecondaryTestCatalog(t)
		original, err := NewSecondaryRecord(ct, NewSecondaryRecordInput{
			fileId:     page.FileId(2),
			deleteMark: 0,
			indexName:  "idx_name",
			colNames:   []string{"name"},
			values:     []string{"Alice"},
			pk:         []string{"1"},
		})
		assert.NoError(t, err)
		encoded := original.Encode()

		// WHEN
		decoded, err := DecodeSecondaryRecord(encoded, ct, page.FileId(2), "idx_name")

		// THEN
		assert.NoError(t, err)
		assert.Equal(t, original.colNames, decoded.colNames)
		assert.Equal(t, original.values, decoded.values)
		assert.Equal(t, original.pk, decoded.pk)
		assert.Equal(t, original.deleteMark, decoded.deleteMark)
	})

	t.Run("削除マーク付きレコードをデコードできる", func(t *testing.T) {
		// GIVEN
		ct := setupSecondaryTestCatalog(t)
		original, err := NewSecondaryRecord(ct, NewSecondaryRecordInput{
			fileId:     page.FileId(2),
			deleteMark: 1,
			indexName:  "idx_name",
			colNames:   []string{"name"},
			values:     []string{"Alice"},
			pk:         []string{"1"},
		})
		assert.NoError(t, err)
		encoded := original.Encode()

		// WHEN
		decoded, err := DecodeSecondaryRecord(encoded, ct, page.FileId(2), "idx_name")

		// THEN
		assert.NoError(t, err)
		assert.Equal(t, byte(1), decoded.deleteMark)
	})

	t.Run("複合セカンダリキーのレコードをデコードできる", func(t *testing.T) {
		// GIVEN
		ct := setupSecondaryTestCatalog(t)
		original, err := NewSecondaryRecord(ct, NewSecondaryRecordInput{
			fileId:     page.FileId(2),
			deleteMark: 0,
			indexName:  "idx_name_email",
			colNames:   []string{"name", "email"},
			values:     []string{"Alice", "alice@example.com"},
			pk:         []string{"1"},
		})
		assert.NoError(t, err)
		encoded := original.Encode()

		// WHEN
		decoded, err := DecodeSecondaryRecord(encoded, ct, page.FileId(2), "idx_name_email")

		// THEN
		assert.NoError(t, err)
		assert.Equal(t, original.colNames, decoded.colNames)
		assert.Equal(t, original.values, decoded.values)
		assert.Equal(t, original.pk, decoded.pk)
	})

	t.Run("デコードされたキーの長さがインデックスカラム数未満の場合エラーを返す", func(t *testing.T) {
		// GIVEN
		ct := setupSecondaryTestCatalog(t)
		record := btree.NewRecord([]byte{0x00}, nil, nil)

		// WHEN
		_, err := DecodeSecondaryRecord(record, ct, page.FileId(2), "idx_name")

		// THEN
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "decoded key length")
	})

	t.Run("存在しないインデックス名を指定するとエラーを返す", func(t *testing.T) {
		// GIVEN
		ct := setupSecondaryTestCatalog(t)
		record := btree.NewRecord([]byte{0x00}, []byte{}, nil)

		// WHEN
		_, err := DecodeSecondaryRecord(record, ct, page.FileId(2), "nonexistent")

		// THEN
		assert.Error(t, err)
	})
}

// setupSecondaryTestCatalog はセカンダリインデックスのテスト用カタログを作成する
//
// テーブル: FileId=2, カラム (id:0, name:1, email:2)
// インデックス:
//   - idx_name: NonUnique, カラム (name:0)
//   - idx_email: Unique, カラム (email:0)
//   - idx_name_email: NonUnique, カラム (name:0, email:1)
func setupSecondaryTestCatalog(t *testing.T) *dictionary.Catalog {
	t.Helper()
	path := filepath.Join(t.TempDir(), "secondary_test.db")
	fileId := page.FileId(0)
	hf, err := file.NewHeapFile(fileId, path)
	if err != nil {
		t.Fatalf("HeapFile の作成に失敗: %v", err)
	}
	t.Cleanup(func() { _ = hf.Close() })
	bp := buffer.NewPool(page.Size * 30)
	bp.RegisterHeapFile(fileId, hf)

	ct, err := dictionary.CreateCatalog(bp)
	if err != nil {
		t.Fatalf("Catalog の作成に失敗: %v", err)
	}

	tableFileId := page.FileId(2)
	dummyPageId := page.NewId(tableFileId, page.PageNumber(0))
	_ = ct.TableMeta().Insert(dictionary.NewTableMetaRecord("users", dummyPageId, 3))
	_ = ct.ColumnMeta().Insert(dictionary.NewColumnMetaRecord(tableFileId, "id", 0))
	_ = ct.ColumnMeta().Insert(dictionary.NewColumnMetaRecord(tableFileId, "name", 1))
	_ = ct.ColumnMeta().Insert(dictionary.NewColumnMetaRecord(tableFileId, "email", 2))

	// PRIMARY: プライマリインデックス, カラム (id)
	indexId0 := dictionary.IndexId(0)
	_ = ct.IndexMeta().Insert(dictionary.NewIndexMetaRecord(tableFileId, indexId0, dictionary.PrimaryIndexName, dictionary.IndexTypePrimary, 1, dummyPageId))
	_ = ct.IndexKeyColumnMeta().Insert(dictionary.NewIndexKeyColumnMetaRecord(indexId0, "id", 0))

	indexId1 := dictionary.IndexId(1)
	_ = ct.IndexMeta().Insert(dictionary.NewIndexMetaRecord(tableFileId, indexId1, "idx_name", dictionary.IndexTypeNonUnique, 1, dummyPageId))
	_ = ct.IndexKeyColumnMeta().Insert(dictionary.NewIndexKeyColumnMetaRecord(indexId1, "name", 0))

	indexId2 := dictionary.IndexId(2)
	_ = ct.IndexMeta().Insert(dictionary.NewIndexMetaRecord(tableFileId, indexId2, "idx_email", dictionary.IndexTypeUnique, 1, dummyPageId))
	_ = ct.IndexKeyColumnMeta().Insert(dictionary.NewIndexKeyColumnMetaRecord(indexId2, "email", 0))

	indexId3 := dictionary.IndexId(3)
	_ = ct.IndexMeta().Insert(dictionary.NewIndexMetaRecord(tableFileId, indexId3, "idx_name_email", dictionary.IndexTypeNonUnique, 2, dummyPageId))
	_ = ct.IndexKeyColumnMeta().Insert(dictionary.NewIndexKeyColumnMetaRecord(indexId3, "name", 0))
	_ = ct.IndexKeyColumnMeta().Insert(dictionary.NewIndexKeyColumnMetaRecord(indexId3, "email", 1))

	return ct
}
