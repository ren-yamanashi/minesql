package node

import (
	"bytes"
	"encoding/binary"
)

// B+Tree のリーフノードに格納されるキーと値のペア
type Pair struct {
	Key   []byte
	Value []byte
}

func NewPair(key []byte, value []byte) Pair {
	return Pair{
		Key:   key,
		Value: value,
	}
}

// key-value ペアをバイト列にシリアライズする
// フォーマット: [key_size(4 bytes][key][value]
func (p *Pair) ToBytes() []byte {
	keySize := uint32(len(p.Key))
	valueSize := uint32(len(p.Value))
	data := make([]byte, 4+keySize+valueSize)

	binary.LittleEndian.PutUint32(data[0:4], keySize)
	copy(data[4:4+len(p.Key)], p.Key)
	copy(data[4+len(p.Key):], p.Value)

	return data
}

// Pair のキーと、指定されたキーを比較する
// 戻り値:
// -1: p.Key < otherKey
// 0:  p.Key == otherKey
// 1:  p.Key > otherKey
func (p Pair) CompareKey(otherKey []byte) int {
	return bytes.Compare(p.Key, otherKey)
}

// バイト列から key-value ペアを復元する
// フォーマット: [key_size(4 bytes][key][value]
func PairFromBytes(data []byte) Pair {
	if len(data) < 4 {
		return Pair{}
	}

	keySize := binary.LittleEndian.Uint32(data[0:4])

	// キー部分がデータ長を超えている場合は不正なデータなので、空のペアを返す
	if len(data) < int(4+keySize) {
		return Pair{}
	}

	return Pair{
		Key:   data[4 : 4+keySize],
		Value: data[4+keySize:],
	}
}
