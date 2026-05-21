package access

import (
	"encoding/binary"
	"errors"
	"fmt"

	"github.com/ren-yamanashi/minesql/internal/storage/btree/node"
	"github.com/ren-yamanashi/minesql/internal/storage/catalog"
	"github.com/ren-yamanashi/minesql/internal/storage/encode"
	"github.com/ren-yamanashi/minesql/internal/storage/lock"
	"github.com/ren-yamanashi/minesql/internal/storage/page"
	"github.com/ren-yamanashi/minesql/internal/storage/undo"
)

type newPrimaryRecordInput struct {
	fileId     page.FileId
	pkCount    int
	deleteMark byte
	lastTrxId  lock.TrxId
	rollPtr    undo.Pointer
	colNames   []string // テーブルを構成するカラムのリスト
	values     []string // テーブルを構成するカラム値のリスト (lastTrxId, rollPtr は含まない)
}

// primaryRecord はプライマリインデックスレコード
type primaryRecord struct {
	pkCount    int
	deleteMark byte
	lastTrxId  lock.TrxId
	rollPtr    undo.Pointer
	ColNames   []string
	Values     []string
}

func newPrimaryRecord(ct *catalog.Catalog, input newPrimaryRecordInput) (*primaryRecord, error) {
	if len(input.colNames) != len(input.values) {
		return nil, errors.New("number of colNames not equal values")
	}
	return sortPrimaryRecord(ct, input)
}

// update は指定されたカラムの値を更新した新しい PrimaryRecord を返す
// (colNames はテーブルの全カラムである必要はない)
func (r *primaryRecord) update(trxId lock.TrxId, colNames, values []string) (*primaryRecord, error) {
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

	return &primaryRecord{
		pkCount:    r.pkCount,
		deleteMark: r.deleteMark,
		lastTrxId:  trxId,
		rollPtr:    r.rollPtr,
		ColNames:   newColNames,
		Values:     newValues,
	}, nil
}

// setRollPtr は rollPtr をセットする
func (r *primaryRecord) setRollPtr(rollPtr undo.Pointer) {
	r.rollPtr = rollPtr
}

// encode は node.Record にエンコードする
//   - 非キー領域: lastTrxId (4B) + rollPtr (6B) + 非キーカラム
func (r *primaryRecord) encode() node.Record {
	var key []byte
	encode.Encode(stringToByteSlice(r.Values[:r.pkCount]), &key)

	var nonKey []byte
	nonKey = binary.BigEndian.AppendUint32(nonKey, r.lastTrxId)
	nonKey = append(nonKey, r.rollPtr.Encode()...)
	encode.Encode(stringToByteSlice(r.Values[r.pkCount:]), &nonKey)

	return node.NewRecord([]byte{r.deleteMark}, key, nonKey)
}

// decodePrimaryRecord は node.Record から PrimaryRecord にデコードする
//   - 非キー領域: lastTrxId (4B) + rollPtr (6B) + 非キーカラム
func decodePrimaryRecord(record node.Record, ct *catalog.Catalog, fileId page.FileId) (*primaryRecord, error) {
	var values [][]byte
	encode.Decode(record.Key(), &values)
	pkCount := len(values)

	// 非キー領域から lastTrxId と rollPtr を読み取る
	nonKey := record.NonKey()
	const systemFieldsSize = lock.TrxIdSize + undo.PointerSize
	if len(nonKey) < systemFieldsSize {
		return nil, fmt.Errorf("non-key data too short: got %d bytes, need at least %d", len(nonKey), systemFieldsSize)
	}
	lastTrxId := binary.BigEndian.Uint32(nonKey[:lock.TrxIdSize])
	rollPtr, err := undo.DecodePointer(nonKey[lock.TrxIdSize:systemFieldsSize])
	if err != nil {
		return nil, err
	}

	// 残りの非キー領域からカラムデータをデコード
	encode.Decode(nonKey[systemFieldsSize:], &values)

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

	return &primaryRecord{
		pkCount:    pkCount,
		deleteMark: record.Header()[0],
		lastTrxId:  lastTrxId,
		rollPtr:    rollPtr,
		ColNames:   colNames,
		Values:     byteSliceToString(values),
	}, nil
}

// sortPrimaryRecord はカラムメタデータを参照して、レコードをテーブル定義順に並び替える
func sortPrimaryRecord(ct *catalog.Catalog, input newPrimaryRecordInput) (*primaryRecord, error) {
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

	return &primaryRecord{
		pkCount:    input.pkCount,
		deleteMark: input.deleteMark,
		lastTrxId:  input.lastTrxId,
		rollPtr:    input.rollPtr,
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
