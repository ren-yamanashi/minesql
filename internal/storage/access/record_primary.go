package access

import (
	"encoding/binary"
	"errors"
	"fmt"

	"github.com/ren-yamanashi/minesql/internal/storage/btree"
	"github.com/ren-yamanashi/minesql/internal/storage/dictionary"
	"github.com/ren-yamanashi/minesql/internal/storage/encode"
	"github.com/ren-yamanashi/minesql/internal/storage/lock"
	"github.com/ren-yamanashi/minesql/internal/storage/page"
	"github.com/ren-yamanashi/minesql/internal/storage/undo"
)

var errColNameValueMismatch = errors.New("number of colNames not equal values")

type NewPrimaryRecordInput struct {
	fileId     page.FileId
	pkCount    int
	deleteMark byte
	lastTrxId  lock.TrxId
	rollPtr    undo.Pointer
	colNames   []string // テーブルを構成するカラムのリスト
	values     []string // テーブルを構成するカラム値のリスト (lastTrxId, rollPtr は含まない)
}

type PrimaryRecord struct {
	pkCount    int
	deleteMark byte
	lastTrxId  lock.TrxId
	rollPtr    undo.Pointer
	colNames   []string
	values     []string
}

func NewPrimaryRecord(ct *dictionary.Catalog, input NewPrimaryRecordInput) (*PrimaryRecord, error) {
	if len(input.colNames) != len(input.values) {
		return nil, errColNameValueMismatch
	}
	return sortPrimaryRecord(ct, input)
}

// Encode は btree.Record にエンコードする
//   - 非キー領域: lastTrxId (4B) + rollPtr (6B) + 非キーカラム
func (r *PrimaryRecord) Encode() btree.Record {
	key := encode.Encode(nil, stringToByteSlice(r.values[:r.pkCount]))

	var nonKey []byte
	nonKey = binary.BigEndian.AppendUint32(nonKey, uint32(r.lastTrxId))
	nonKey = append(nonKey, r.rollPtr.Encode()...)
	nonKey = encode.Encode(nonKey, stringToByteSlice(r.values[r.pkCount:]))

	return btree.NewRecord([]byte{r.deleteMark}, key, nonKey)
}

// SecondaryKey はセカンダリインデックスの B+Tree キー (SK+PK) を構築する
func (r *PrimaryRecord) SecondaryKey(keyCols map[string]int) []byte {
	valMap := make(map[string]string, len(r.colNames))
	for i, name := range r.colNames {
		valMap[name] = r.values[i]
	}

	// SK をインデックス定義順に取得
	skValues := make([]string, len(keyCols))
	for name, pos := range keyCols {
		skValues[pos] = valMap[name]
	}

	// PK を取得
	pkValues := r.values[:r.pkCount]

	// SK + PK をエンコード
	var key []byte
	key = encode.Encode(key, stringToByteSlice(skValues))
	key = encode.Encode(key, stringToByteSlice(pkValues))
	return key
}

// update は指定されたカラムの値を更新した新しい PrimaryRecord を返す
// (colNames はテーブルの全カラムである必要はない)
func (r *PrimaryRecord) update(trxId lock.TrxId, colNames, values []string) (*PrimaryRecord, error) {
	if len(colNames) != len(values) {
		return nil, errColNameValueMismatch
	}

	// 既存の PrimaryRecord のカラム名 → 位置のマップを構築
	posMap := map[string]int{}
	for i, name := range r.colNames {
		posMap[name] = i
	}

	newColNames := make([]string, len(r.colNames))
	newValues := make([]string, len(r.values))
	copy(newColNames, r.colNames)
	copy(newValues, r.values)

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
		lastTrxId:  trxId,
		rollPtr:    r.rollPtr,
		colNames:   newColNames,
		values:     newValues,
	}, nil
}

// setRollPtr は rollPtr をセットする
func (r *PrimaryRecord) setRollPtr(rollPtr undo.Pointer) {
	r.rollPtr = rollPtr
}

// DecodePrimaryRecord は btree.Record から PrimaryRecord にデコードする
//   - 非キー領域: lastTrxId (4B) + rollPtr (6B) + 非キーカラム
func DecodePrimaryRecord(record btree.Record, ct *dictionary.Catalog, fileId page.FileId) (*PrimaryRecord, error) {
	values, err := encode.Decode(record.Key())
	if err != nil {
		return nil, err
	}
	pkCount := len(values)

	// 非キー領域から lastTrxId と rollPtr を読み取る
	nonKey := record.NonKey()
	const systemFieldsSize = lock.TrxIdSize + undo.PointerSize
	if len(nonKey) < systemFieldsSize {
		return nil, fmt.Errorf(
			"non-key data too short: got %d bytes, need at least %d",
			len(nonKey), systemFieldsSize,
		)
	}
	lastTrxId := lock.TrxId(binary.BigEndian.Uint32(nonKey[:lock.TrxIdSize]))
	rollPtr, err := undo.DecodePointer(nonKey[lock.TrxIdSize:systemFieldsSize])
	if err != nil {
		return nil, err
	}

	// 残りの非キー領域からカラムデータをデコード
	nonKeyValues, err := encode.Decode(nonKey[systemFieldsSize:])
	if err != nil {
		return nil, err
	}
	values = append(values, nonKeyValues...)

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
		lastTrxId:  lastTrxId,
		rollPtr:    rollPtr,
		colNames:   colNames,
		values:     byteSliceToString(values),
	}, nil
}

// sortPrimaryRecord はカラムメタデータを参照して、レコードをテーブル定義順に並び替える
func sortPrimaryRecord(ct *dictionary.Catalog, input NewPrimaryRecordInput) (*PrimaryRecord, error) {
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
		lastTrxId:  input.lastTrxId,
		rollPtr:    input.rollPtr,
		colNames:   sortedColNames,
		values:     sortedValues,
	}, nil
}

// fetchColumnDefs はカラムメタデータを検索し、カラム名 → テーブル定義上の位置のマップを返す
func fetchColumnDefs(ct *dictionary.Catalog, fileId page.FileId) (map[string]int, error) {
	fileIdBytes := binary.BigEndian.AppendUint32(nil, uint32(fileId))
	iter, err := ct.ColumnMeta().Search(dictionary.SearchModeKey{Key: [][]byte{fileIdBytes}})
	if err != nil {
		return nil, err
	}
	defer iter.Close()

	colDefs := map[string]int{}
	for {
		colRecord, ok, err := iter.Next()
		if err != nil {
			return nil, err
		}
		if !ok {
			break
		}
		if colRecord.FileId() != fileId {
			break
		}
		colDefs[colRecord.Name()] = colRecord.Position()
	}
	return colDefs, nil
}
