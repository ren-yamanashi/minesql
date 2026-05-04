package access

import (
	"path/filepath"
	"testing"

	"github.com/ren-yamanashi/minesql/internal/storage/btree/node"
	"github.com/ren-yamanashi/minesql/internal/storage/buffer"
	"github.com/ren-yamanashi/minesql/internal/storage/catalog"
	"github.com/ren-yamanashi/minesql/internal/storage/encode"
	"github.com/ren-yamanashi/minesql/internal/storage/file"
	"github.com/ren-yamanashi/minesql/internal/storage/page"
	"github.com/stretchr/testify/assert"
)

func TestSecondaryRecordEncode(t *testing.T) {
	t.Run("セカンダリキーとプライマリキーをエンコードしたレコードを返す", func(t *testing.T) {
		// GIVEN
		sr := &SecondaryRecord{
			deleteMark: 0x00,
			Values:     []string{"sk1"},
			Pk:         []string{"pk1"},
		}

		// WHEN
		record := sr.encode()

		// THEN
		assert.Equal(t, []byte{0x00}, record.Header())

		var decoded [][]byte
		encode.Decode(record.Key(), &decoded)
		assert.Equal(t, [][]byte{[]byte("sk1"), []byte("pk1")}, decoded)

		assert.Nil(t, record.NonKey())
	})

	t.Run("複合セカンダリキーを正しくエンコードする", func(t *testing.T) {
		// GIVEN
		sr := &SecondaryRecord{
			deleteMark: 0x00,
			Values:     []string{"sk1", "sk2"},
			Pk:         []string{"pk1"},
		}

		// WHEN
		record := sr.encode()

		// THEN
		var decoded [][]byte
		encode.Decode(record.Key(), &decoded)
		assert.Equal(t, [][]byte{[]byte("sk1"), []byte("sk2"), []byte("pk1")}, decoded)
	})

	t.Run("削除マークが設定される", func(t *testing.T) {
		// GIVEN
		sr := &SecondaryRecord{
			deleteMark: 0x01,
			Values:     []string{"sk1"},
			Pk:         []string{"pk1"},
		}

		// WHEN
		record := sr.encode()

		// THEN
		assert.Equal(t, []byte{0x01}, record.Header())
	})

	t.Run("複合プライマリキーを正しくエンコードする", func(t *testing.T) {
		// GIVEN
		sr := &SecondaryRecord{
			deleteMark: 0x00,
			Values:     []string{"sk1"},
			Pk:         []string{"pk1", "pk2"},
		}

		// WHEN
		record := sr.encode()

		// THEN
		var decoded [][]byte
		encode.Decode(record.Key(), &decoded)
		assert.Equal(t, [][]byte{[]byte("sk1"), []byte("pk1"), []byte("pk2")}, decoded)
	})
}

func TestSecondaryRecordEncodedSecondaryKey(t *testing.T) {
	t.Run("エンコード済みのセカンダリキーのみを返す", func(t *testing.T) {
		// GIVEN
		sr := &SecondaryRecord{
			Values: []string{"sk1"},
			Pk:     []string{"pk1"},
		}

		// WHEN
		result := sr.encodedSecondaryKey()

		// THEN
		var expected []byte
		encode.Encode([][]byte{[]byte("sk1")}, &expected)
		assert.Equal(t, expected, result)
	})

	t.Run("複合セカンダリキーの場合も正しくエンコードする", func(t *testing.T) {
		// GIVEN
		sr := &SecondaryRecord{
			Values: []string{"sk1", "sk2"},
			Pk:     []string{"pk1"},
		}

		// WHEN
		result := sr.encodedSecondaryKey()

		// THEN
		var expected []byte
		encode.Encode([][]byte{[]byte("sk1"), []byte("sk2")}, &expected)
		assert.Equal(t, expected, result)
	})
}

func TestNewSecondaryRecord(t *testing.T) {
	t.Run("カタログを参照してインデックス定義順に並び替えたレコードを返す", func(t *testing.T) {
		// GIVEN
		ct := setupSecondaryTestCatalog(t)

		// WHEN
		sr, err := newSecondaryRecord(ct, newSecondaryRecordInput{
			fileId:     page.FileId(2),
			deleteMark: 0,
			indexName:  "idx_name",
			colNames:   []string{"name"},
			values:     []string{"Alice"},
			pk:         []string{"1"},
		})

		// THEN
		assert.NoError(t, err)
		assert.Equal(t, []string{"name"}, sr.ColNames)
		assert.Equal(t, []string{"Alice"}, sr.Values)
		assert.Equal(t, []string{"1"}, sr.Pk)
		assert.Equal(t, byte(0), sr.deleteMark)
	})

	t.Run("カラム名と値の数が一致しない場合エラーを返す", func(t *testing.T) {
		// GIVEN
		ct := setupSecondaryTestCatalog(t)

		// WHEN
		_, err := newSecondaryRecord(ct, newSecondaryRecordInput{
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
		_, err := newSecondaryRecord(ct, newSecondaryRecordInput{
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
		_, err := newSecondaryRecord(ct, newSecondaryRecordInput{
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
		_, err := newSecondaryRecord(ct, newSecondaryRecordInput{
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

func TestDecodeSecondaryRecord(t *testing.T) {
	t.Run("エンコードしたレコードをデコードすると元のデータに戻る", func(t *testing.T) {
		// GIVEN
		ct := setupSecondaryTestCatalog(t)
		original, err := newSecondaryRecord(ct, newSecondaryRecordInput{
			fileId:     page.FileId(2),
			deleteMark: 0,
			indexName:  "idx_name",
			colNames:   []string{"name"},
			values:     []string{"Alice"},
			pk:         []string{"1"},
		})
		assert.NoError(t, err)
		encoded := original.encode()

		// WHEN
		decoded, err := decodeSecondaryRecord(encoded, ct, page.FileId(2), "idx_name")

		// THEN
		assert.NoError(t, err)
		assert.Equal(t, original.ColNames, decoded.ColNames)
		assert.Equal(t, original.Values, decoded.Values)
		assert.Equal(t, original.Pk, decoded.Pk)
		assert.Equal(t, original.deleteMark, decoded.deleteMark)
	})

	t.Run("削除マーク付きレコードをデコードできる", func(t *testing.T) {
		// GIVEN
		ct := setupSecondaryTestCatalog(t)
		original, err := newSecondaryRecord(ct, newSecondaryRecordInput{
			fileId:     page.FileId(2),
			deleteMark: 1,
			indexName:  "idx_name",
			colNames:   []string{"name"},
			values:     []string{"Alice"},
			pk:         []string{"1"},
		})
		assert.NoError(t, err)
		encoded := original.encode()

		// WHEN
		decoded, err := decodeSecondaryRecord(encoded, ct, page.FileId(2), "idx_name")

		// THEN
		assert.NoError(t, err)
		assert.Equal(t, byte(1), decoded.deleteMark)
	})

	t.Run("複合セカンダリキーのレコードをデコードできる", func(t *testing.T) {
		// GIVEN
		ct := setupSecondaryTestCatalog(t)
		original, err := newSecondaryRecord(ct, newSecondaryRecordInput{
			fileId:     page.FileId(2),
			deleteMark: 0,
			indexName:  "idx_name_email",
			colNames:   []string{"name", "email"},
			values:     []string{"Alice", "alice@example.com"},
			pk:         []string{"1"},
		})
		assert.NoError(t, err)
		encoded := original.encode()

		// WHEN
		decoded, err := decodeSecondaryRecord(encoded, ct, page.FileId(2), "idx_name_email")

		// THEN
		assert.NoError(t, err)
		assert.Equal(t, original.ColNames, decoded.ColNames)
		assert.Equal(t, original.Values, decoded.Values)
		assert.Equal(t, original.Pk, decoded.Pk)
	})

	t.Run("デコードされたキーの長さがインデックスカラム数未満の場合エラーを返す", func(t *testing.T) {
		// GIVEN
		ct := setupSecondaryTestCatalog(t)
		record := node.NewRecord([]byte{0x00}, nil, nil)

		// WHEN
		_, err := decodeSecondaryRecord(record, ct, page.FileId(2), "idx_name")

		// THEN
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "decoded key length")
	})

	t.Run("存在しないインデックス名を指定するとエラーを返す", func(t *testing.T) {
		// GIVEN
		ct := setupSecondaryTestCatalog(t)
		record := node.NewRecord([]byte{0x00}, []byte{}, nil)

		// WHEN
		_, err := decodeSecondaryRecord(record, ct, page.FileId(2), "nonexistent")

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
func setupSecondaryTestCatalog(t *testing.T) *catalog.Catalog {
	t.Helper()
	path := filepath.Join(t.TempDir(), "secondary_test.db")
	fileId := page.FileId(0)
	hf, err := file.NewHeapFile(fileId, path)
	if err != nil {
		t.Fatalf("HeapFile の作成に失敗: %v", err)
	}
	t.Cleanup(func() { _ = hf.Close() })
	bp := buffer.NewBufferPool(page.PageSize * 30)
	bp.RegisterHeapFile(fileId, hf)

	ct, err := catalog.CreateCatalog(bp)
	if err != nil {
		t.Fatalf("Catalog の作成に失敗: %v", err)
	}

	tableFileId := page.FileId(2)
	dummyPageId := page.NewPageId(tableFileId, page.PageNumber(0))
	_ = ct.TableMeta.Insert("users", dummyPageId, 3)
	_ = ct.ColumnMeta.Insert(tableFileId, "id", 0)
	_ = ct.ColumnMeta.Insert(tableFileId, "name", 1)
	_ = ct.ColumnMeta.Insert(tableFileId, "email", 2)

	// PRIMARY: プライマリインデックス, カラム (id)
	indexId0 := catalog.IndexId(0)
	_ = ct.IndexMeta.Insert(tableFileId, catalog.PrimaryIndexName, indexId0, catalog.IndexTypePrimary, 1, dummyPageId)
	_ = ct.IndexKeyColMeta.Insert(indexId0, "id", 0)

	indexId1 := catalog.IndexId(1)
	_ = ct.IndexMeta.Insert(tableFileId, "idx_name", indexId1, catalog.IndexTypeNonUnique, 1, dummyPageId)
	_ = ct.IndexKeyColMeta.Insert(indexId1, "name", 0)

	indexId2 := catalog.IndexId(2)
	_ = ct.IndexMeta.Insert(tableFileId, "idx_email", indexId2, catalog.IndexTypeUnique, 1, dummyPageId)
	_ = ct.IndexKeyColMeta.Insert(indexId2, "email", 0)

	indexId3 := catalog.IndexId(3)
	_ = ct.IndexMeta.Insert(tableFileId, "idx_name_email", indexId3, catalog.IndexTypeNonUnique, 2, dummyPageId)
	_ = ct.IndexKeyColMeta.Insert(indexId3, "name", 0)
	_ = ct.IndexKeyColMeta.Insert(indexId3, "email", 1)

	return ct
}
