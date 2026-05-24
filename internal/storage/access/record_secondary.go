package access

import (
	"encoding/binary"
	"fmt"

	"github.com/ren-yamanashi/minesql/internal/storage/btree"
	"github.com/ren-yamanashi/minesql/internal/storage/dictionary"
	"github.com/ren-yamanashi/minesql/internal/storage/encode"
	"github.com/ren-yamanashi/minesql/internal/storage/page"
)

type NewSecondaryRecordInput struct {
	fileId     page.FileId
	deleteMark byte
	indexName  string
	colNames   []string // インデックスを構成するカラム名のリスト
	values     []string // インデックスを構成するカラム値のリスト (SK)
	pk         []string // プライマリキー
}

type SecondaryRecord struct {
	deleteMark byte
	colNames   []string // インデックスを構成するカラム名のリスト
	values     []string // インデックスを構成するカラム値のリスト (SK)
	pk         []string // プライマリキー
}

func NewSecondaryRecord(ct *dictionary.Catalog, input NewSecondaryRecordInput) (*SecondaryRecord, error) {
	if len(input.colNames) != len(input.values) {
		return nil, errColNameValueMismatch
	}
	return sortSecondaryRecord(ct, input)
}

func (r *SecondaryRecord) Encode() btree.Record {
	var key []byte
	key = encode.Encode(key, stringToByteSlice(r.values))
	key = encode.Encode(key, stringToByteSlice(r.pk))
	return btree.NewRecord([]byte{r.deleteMark}, key, nil)
}

func (r *SecondaryRecord) encodedSecondaryKey() []byte {
	return encode.Encode(nil, stringToByteSlice(r.values))
}

func DecodeSecondaryRecord(
	record btree.Record,
	ct *dictionary.Catalog,
	fileId page.FileId,
	indexName string,
) (*SecondaryRecord, error) {
	index, err := fetchIndex(ct, fileId, indexName)
	if err != nil {
		return nil, err
	}
	keyCols, err := fetchIndexKeyColumn(ct, index.IndexId())
	if err != nil {
		return nil, err
	}

	key, err := encode.Decode(record.Key())
	if err != nil {
		return nil, err
	}
	if len(key) < index.ColumnCount() {
		return nil, fmt.Errorf(
			"decoded key length %d is less than index column count %d",
			len(key), index.ColumnCount(),
		)
	}
	sk := key[:index.ColumnCount()]
	pk := key[index.ColumnCount():]

	if len(sk) != len(keyCols) {
		return nil, fmt.Errorf(
			"index key column count mismatch: got %d values, expected %d",
			len(sk), len(keyCols),
		)
	}

	colNames := make([]string, len(keyCols))
	for name, pos := range keyCols {
		colNames[pos] = name
	}

	return &SecondaryRecord{
		deleteMark: record.Header()[0],
		colNames:   colNames,
		values:     byteSliceToString(sk),
		pk:         byteSliceToString(pk),
	}, nil
}

// sortSecondaryRecord はメタデータを参照して、レコードをインデックス定義順に並び替える
func sortSecondaryRecord(ct *dictionary.Catalog, input NewSecondaryRecordInput) (*SecondaryRecord, error) {
	index, err := fetchIndex(ct, input.fileId, input.indexName)
	if err != nil {
		return nil, err
	}
	keyCols, err := fetchIndexKeyColumn(ct, index.IndexId())
	if err != nil {
		return nil, err
	}
	if len(input.values) != len(keyCols) {
		return nil, fmt.Errorf(
			"index key column count mismatch: got %d values, expected %d",
			len(input.values), len(keyCols),
		)
	}

	sortedColNames := make([]string, len(keyCols))
	sortedValues := make([]string, len(keyCols))
	seen := map[string]bool{}
	for i, name := range input.colNames {
		if seen[name] {
			return nil, fmt.Errorf("duplicate index key column %q", name)
		}
		seen[name] = true
		pos, ok := keyCols[name]
		if !ok {
			return nil, fmt.Errorf("index key column %q not found in %q", name, input.indexName)
		}
		sortedColNames[pos] = name
		sortedValues[pos] = input.values[i]
	}

	return &SecondaryRecord{
		deleteMark: input.deleteMark,
		colNames:   sortedColNames,
		values:     sortedValues,
		pk:         input.pk,
	}, nil
}

// fetchIndex はインデックスメタデータを検索し、指定された名前のインデックスレコードを返す
func fetchIndex(ct *dictionary.Catalog, fileId page.FileId, indexName string) (dictionary.IndexRecord, error) {
	fileIdBytes := binary.BigEndian.AppendUint32(nil, uint32(fileId))
	iter, err := ct.IndexMeta().Search(dictionary.SearchModeKey{Key: [][]byte{fileIdBytes, []byte(indexName)}})
	if err != nil {
		return dictionary.IndexRecord{}, err
	}
	defer iter.Close()
	indexRecord, ok, err := iter.Next()
	if err != nil {
		return dictionary.IndexRecord{}, err
	}
	if !ok || indexRecord.FileId() != fileId || indexRecord.Name() != indexName {
		return dictionary.IndexRecord{}, fmt.Errorf("index %q not found", indexName)
	}
	return indexRecord, nil
}

// fetchIndexKeyColumn はインデックスキーカラムメタデータを検索し、カラム名 → インデックス上のカラム位置のマップを返す
func fetchIndexKeyColumn(ct *dictionary.Catalog, indexId dictionary.IndexId) (map[string]int, error) {
	indexIdBytes := binary.BigEndian.AppendUint32(nil, uint32(indexId))
	keyColMetaIter, err := ct.IndexKeyColumnMeta().Search(dictionary.SearchModeKey{Key: [][]byte{indexIdBytes}})
	if err != nil {
		return nil, err
	}
	defer keyColMetaIter.Close()

	keyCols := map[string]int{}
	for {
		keyColRecord, ok, err := keyColMetaIter.Next()
		if err != nil {
			return nil, err
		}
		if !ok {
			break
		}
		if keyColRecord.IndexId() != indexId {
			break
		}
		keyCols[keyColRecord.Name()] = keyColRecord.Position()
	}
	return keyCols, nil
}
