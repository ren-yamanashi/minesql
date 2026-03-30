package access

import "minesql/internal/encode"

type RecordHeader struct {
	DeleteMark uint8
}

type RecordNonKey struct {
	NonKeyColumns [][]byte
}

// Record はエグゼキュータからアクセスメソッドに渡されるレコード
type Record struct {
	Header RecordHeader
	Key    [][]byte
	NonKey RecordNonKey
}

// NewRecord は全カラム値のフラット配列と PrimaryKeyCount から Record を生成する
//
// DeleteMark は 0 で初期化される
func NewRecord(columns [][]byte, primaryKeyCount uint8) Record {
	return Record{
		Header: RecordHeader{DeleteMark: 0},
		Key:    columns[:primaryKeyCount],
		NonKey: RecordNonKey{NonKeyColumns: columns[primaryKeyCount:]},
	}
}

// Columns は Key と NonKeyColumns を結合した全カラム値のフラット配列を返す
func (r *Record) Columns() [][]byte {
	cols := make([][]byte, 0, len(r.Key)+len(r.NonKey.NonKeyColumns))
	cols = append(cols, r.Key...)
	cols = append(cols, r.NonKey.NonKeyColumns...)
	return cols
}

// EncodeKey は Key を Memcomparable format でエンコードする
func (r *Record) EncodeKey() []byte {
	var encoded []byte
	encode.Encode(r.Key, &encoded)
	return encoded
}

// EncodeHeader は Header をバイト列にエンコードする
func (r *Record) EncodeHeader() []byte {
	return []byte{r.Header.DeleteMark}
}

// EncodeNonKey は NonKeyColumns を Memcomparable format でエンコードする
func (r *Record) EncodeNonKey() []byte {
	var encoded []byte
	encode.Encode(r.NonKey.NonKeyColumns, &encoded)
	return encoded
}
