package node

import (
	"bytes"
	"encoding/binary"
)

// Record は B+Tree のノードに格納されるレコード
//
// 3 領域で構成される:
//   - record[0]: ヘッダー (例: []byte{deleteMark})
//   - record[1]: キーフィールド (memcomparable エンコード済み)
//   - record[2]: 非キーフィールド (memcomparable エンコード済み、nil 可)
type Record [][]byte

// NewRecord はヘッダー、キー、非キーから新しいレコードを作成する
func NewRecord(header, key, nonKey []byte) Record {
	return Record{header, key, nonKey}
}

// HeaderBytes はヘッダー領域を返す
func (r Record) HeaderBytes() []byte { return r[0] }

// KeyBytes はキーフィールド領域を返す
func (r Record) KeyBytes() []byte { return r[1] }

// NonKeyBytes は非キーフィールド領域を返す
func (r Record) NonKeyBytes() []byte { return r[2] }

// CompareKey はレコードのキーと指定されたキーを比較する
//
// 戻り値:
//   - -1: record.Key < otherKey
//   - 0:  record.Key == otherKey
//   - 1:  record.Key > otherKey
func (r Record) CompareKey(otherKey []byte) int {
	return bytes.Compare(r[1], otherKey)
}

// ToBytes はレコードをバイト列にシリアライズする
//   - フォーマット: [headerSize(2B)][keySize(2B)][header][key][nonKey]
func (r Record) ToBytes() []byte {
	headerLen := len(r[0])
	keyLen := len(r[1])
	nonKeyLen := len(r[2])

	data := make([]byte, 4+headerLen+keyLen+nonKeyLen)
	binary.BigEndian.PutUint16(data[0:2], uint16(headerLen))
	binary.BigEndian.PutUint16(data[2:4], uint16(keyLen))

	offset := 4
	copy(data[offset:offset+headerLen], r[0])
	offset += headerLen
	copy(data[offset:offset+keyLen], r[1])
	offset += keyLen
	copy(data[offset:], r[2])

	return data
}

// recordFromBytes はバイト列からレコードを復元する
//   - フォーマット: [headerSize(2B)][keySize(2B)][header][key][nonKey]
func recordFromBytes(data []byte) Record {
	if len(data) < 4 {
		return NewRecord(nil, nil, nil)
	}

	headerSize := int(binary.BigEndian.Uint16(data[0:2]))
	keySize := int(binary.BigEndian.Uint16(data[2:4]))

	headerEnd := 4 + headerSize
	keyEnd := headerEnd + keySize

	if len(data) < keyEnd {
		return NewRecord(nil, nil, nil)
	}

	header := data[4:headerEnd]
	key := data[headerEnd:keyEnd]
	var nonKey []byte
	if keyEnd < len(data) {
		nonKey = data[keyEnd:]
	}

	return NewRecord(header, key, nonKey)
}
