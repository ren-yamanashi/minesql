package access

import (
	"encoding/binary"
	"errors"
	"fmt"

	"github.com/ren-yamanashi/minesql/internal/storage/btree/node"
	"github.com/ren-yamanashi/minesql/internal/storage/catalog"
	"github.com/ren-yamanashi/minesql/internal/storage/encode"
	"github.com/ren-yamanashi/minesql/internal/storage/page"
)

type newSecondaryRecordInput struct {
	fileId     page.FileId
	deleteMark byte
	indexName  string
	colNames   []string // インデックスを構成するカラム名のリスト
	values     []string // インデックスを構成するカラム値のリスト (SK)
	pk         []string // プライマリキー
}

// SecondaryRecord はセカンダリインデックスレコード
type SecondaryRecord struct {
	deleteMark byte
	indexName  string
	ColNames   []string // インデックスを構成するカラム名のリスト
	Values     []string // インデックスを構成するカラム値のリスト (SK)
	Pk         []string // プライマリキー
}

func newSecondaryRecord(ct *catalog.Catalog, input newSecondaryRecordInput) (*SecondaryRecord, error) {
	if len(input.colNames) != len(input.values) {
		return nil, errors.New("number of colNames not equal values")
	}
	return sortSecondaryRecord(ct, input)
}

// encode は node.Record にエンコードする
// キー領域は SK + PK を連結したもの
func (sr *SecondaryRecord) encode() node.Record {
	var key []byte
	encode.Encode(stringToByteSlice(sr.Values), &key)
	encode.Encode(stringToByteSlice(sr.Pk), &key)
	return node.NewRecord([]byte{sr.deleteMark}, key, nil)
}

// encodedSecondaryKey はエンコード済みのセカンダリキーを返す
//
// B+Tree 上のキー (SK + PK) ではなく SK のみ
func (sr *SecondaryRecord) encodedSecondaryKey() []byte {
	var sk []byte
	encode.Encode(stringToByteSlice(sr.Values), &sk)
	return sk
}

// decodeSecondaryRecord は node.Record から SecondaryRecord にデコードする
func decodeSecondaryRecord(
	record node.Record,
	ct *catalog.Catalog,
	fileId page.FileId,
	indexName string,
) (*SecondaryRecord, error) {
	index, err := fetchIndex(ct, fileId, indexName)
	if err != nil {
		return nil, err
	}
	keyCols, err := fetchIndexKeyCol(ct, index.IndexId)
	if err != nil {
		return nil, err
	}

	var key [][]byte
	encode.Decode(record.Key(), &key)
	if len(key) < index.NumOfCol {
		return nil, fmt.Errorf("decoded key length %d is less than index column count %d", len(key), index.NumOfCol)
	}
	sk := key[:index.NumOfCol]
	pk := key[index.NumOfCol:]

	if len(sk) != len(keyCols) {
		return nil, fmt.Errorf("index key column count mismatch: got %d values, expected %d", len(sk), len(keyCols))
	}

	colNames := make([]string, len(keyCols))
	for name, pos := range keyCols {
		colNames[pos] = name
	}

	return &SecondaryRecord{
		deleteMark: record.Header()[0],
		indexName:  indexName,
		ColNames:   colNames,
		Values:     byteSliceToString(sk),
		Pk:         byteSliceToString(pk),
	}, nil
}

// sortSecondaryRecord はメタデータを参照して、レコードをインデックス定義順に並び替える
func sortSecondaryRecord(ct *catalog.Catalog, input newSecondaryRecordInput) (*SecondaryRecord, error) {
	index, err := fetchIndex(ct, input.fileId, input.indexName)
	if err != nil {
		return nil, err
	}
	keyCols, err := fetchIndexKeyCol(ct, index.IndexId)
	if err != nil {
		return nil, err
	}
	if len(input.values) != len(keyCols) {
		return nil, fmt.Errorf("index key column count mismatch: got %d values, expected %d", len(input.values), len(keyCols))
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
		indexName:  input.indexName,
		ColNames:   sortedColNames,
		Values:     sortedValues,
		Pk:         input.pk,
	}, nil
}

// fetchIndex はインデックスメタデータを検索し、指定された名前のインデックスレコードを返す
func fetchIndex(ct *catalog.Catalog, fileId page.FileId, indexName string) (catalog.IndexRecord, error) {
	fileIdBytes := binary.BigEndian.AppendUint32(nil, uint32(fileId))
	iter, err := ct.IndexMeta.Search(catalog.SearchModeKey{Key: [][]byte{fileIdBytes, []byte(indexName)}})
	if err != nil {
		return catalog.IndexRecord{}, err
	}
	indexRecord, ok, err := iter.Next()
	if err != nil {
		return catalog.IndexRecord{}, err
	}
	if !ok || indexRecord.FileId != fileId || indexRecord.Name != indexName {
		return catalog.IndexRecord{}, fmt.Errorf("index %q not found", indexName)
	}
	return indexRecord, nil
}

// fetchIndexKeyCol はインデックスキーカラムメタデータを検索し、カラム名 → インデックス上のカラム位置のマップを返す
func fetchIndexKeyCol(ct *catalog.Catalog, indexId catalog.IndexId) (map[string]int, error) {
	indexIdBytes := binary.BigEndian.AppendUint32(nil, uint32(indexId))
	keyColMetaIter, err := ct.IndexKeyColMeta.Search(catalog.SearchModeKey{Key: [][]byte{indexIdBytes}})
	if err != nil {
		return nil, err
	}

	keyCols := map[string]int{}
	for {
		keyColRecord, ok, err := keyColMetaIter.Next()
		if err != nil {
			return nil, err
		}
		if !ok {
			break
		}
		if keyColRecord.IndexId != indexId {
			break
		}
		keyCols[keyColRecord.Name] = keyColRecord.Pos
	}
	return keyCols, nil
}
