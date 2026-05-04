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

type newPrimaryRecordInput struct {
	fileId     page.FileId
	pkCount    int
	deleteMark byte
	colNames   []string // テーブルを構成するカラムのリスト
	values     []string // テーブルを構成するカラム値のリスト
}

// PrimaryRecord はプライマリインデックスレコード
type PrimaryRecord struct {
	pkCount    int
	deleteMark byte
	ColNames   []string
	Values     []string
}

func newPrimaryRecord(ct *catalog.Catalog, input newPrimaryRecordInput) (*PrimaryRecord, error) {
	if len(input.colNames) != len(input.values) {
		return nil, errors.New("number of colNames not equal values")
	}
	return sortPrimaryRecord(ct, input)
}

// update は指定されたカラムの値を更新した新しい PrimaryRecord を返す
// (colNames はテーブルの全カラムである必要はない)
func (r *PrimaryRecord) update(colNames, values []string) (*PrimaryRecord, error) {
	if len(colNames) != len(values) {
		return nil, errors.New("number of colNames not equal values")
	}

	// 既存の PrimaryRecord のカラム名 → 位置のマップを構築
	posMap := map[string]int{}
	for i, name := range r.ColNames {
		posMap[name] = i
	}

	newColNames := make([]string, len(r.ColNames))
	newValues := make([]string, len(r.Values))
	copy(newColNames, r.ColNames)
	copy(newValues, r.Values)

	seen := map[string]bool{}
	for i, name := range colNames {
		if seen[name] {
			return nil, fmt.Errorf("duplicate column %q", name)
		}
		seen[name] = true
		pos, ok := posMap[name]
		if !ok {
			return nil, fmt.Errorf("column %q not found in record", name)
		}
		newValues[pos] = values[i]
	}

	return &PrimaryRecord{
		pkCount:    r.pkCount,
		deleteMark: r.deleteMark,
		ColNames:   newColNames,
		Values:     newValues,
	}, nil
}

// encode は node.Record にエンコードする
func (pr *PrimaryRecord) encode() node.Record {
	var key []byte
	var nonKey []byte
	encode.Encode(stringToByteSlice(pr.Values[:pr.pkCount]), &key)
	encode.Encode(stringToByteSlice(pr.Values[pr.pkCount:]), &nonKey)
	return node.NewRecord([]byte{pr.deleteMark}, key, nonKey)
}

// decodeRecord は node.Record から PrimaryRecord にデコードする
func decodePrimaryRecord(record node.Record, ct *catalog.Catalog, fileId page.FileId) (*PrimaryRecord, error) {
	var values [][]byte
	encode.Decode(record.Key(), &values)
	pkCount := len(values)
	encode.Decode(record.NonKey(), &values)

	colDefs, err := fetchColumnDefs(ct, fileId)
	if err != nil {
		return nil, err
	}
	if len(values) != len(colDefs) {
		return nil, fmt.Errorf("column count mismatch: got %d values, expected %d", len(values), len(colDefs))
	}

	colNames := make([]string, len(colDefs))
	for name, pos := range colDefs {
		colNames[pos] = name
	}

	return &PrimaryRecord{
		pkCount:    pkCount,
		deleteMark: record.Header()[0],
		ColNames:   colNames,
		Values:     byteSliceToString(values),
	}, nil
}

// sortPrimaryRecord はカラムメタデータを参照して、レコードをテーブル定義順に並び替える
func sortPrimaryRecord(ct *catalog.Catalog, input newPrimaryRecordInput) (*PrimaryRecord, error) {
	colDefs, err := fetchColumnDefs(ct, input.fileId)
	if err != nil {
		return nil, err
	}
	if len(input.colNames) != len(colDefs) {
		return nil, fmt.Errorf("column count mismatch: got %d columns, expected %d", len(input.colNames), len(colDefs))
	}

	sortedColNames := make([]string, len(colDefs))
	sortedValues := make([]string, len(colDefs))
	seen := map[string]bool{}
	for i, name := range input.colNames {
		if seen[name] {
			return nil, fmt.Errorf("duplicate column %q", name)
		}
		seen[name] = true
		pos, ok := colDefs[name]
		if !ok {
			return nil, fmt.Errorf("column %q not found in table definition", name)
		}
		sortedColNames[pos] = name
		sortedValues[pos] = input.values[i]
	}

	return &PrimaryRecord{
		pkCount:    input.pkCount,
		deleteMark: input.deleteMark,
		ColNames:   sortedColNames,
		Values:     sortedValues,
	}, nil
}

// fetchColumnDefs はカラムメタデータを検索し、カラム名 → テーブル定義上の位置のマップを返す
func fetchColumnDefs(ct *catalog.Catalog, fileId page.FileId) (map[string]int, error) {
	fileIdBytes := binary.BigEndian.AppendUint32(nil, uint32(fileId))
	iter, err := ct.ColumnMeta.Search(catalog.SearchModeKey{Key: [][]byte{fileIdBytes}})
	if err != nil {
		return nil, err
	}

	colDefs := map[string]int{}
	for {
		colRecord, ok, err := iter.Next()
		if err != nil {
			return nil, err
		}
		if !ok {
			break
		}
		if colRecord.FileId != fileId {
			break
		}
		colDefs[colRecord.Name] = colRecord.Pos
	}
	return colDefs, nil
}
