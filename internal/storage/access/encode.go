package access

// Encode は複数のバイト列を連結してエンコードする
//
// elements: エンコード対象のバイト列のスライス
//
// dest: エンコード結果の格納先のポインタ
func Encode(elements [][]byte, dest *[]byte) {
	for _, element := range elements {
		size := encodedSize(len(element))

		// dest の容量が、必要なサイズを満たしていない場合は拡張
		if cap(*dest)-len(*dest) < size {
			newData := make([]byte, len(*dest), len(*dest)+size)
			copy(newData, *dest)
			*dest = newData
		}

		// エンコード
		encodeToMemcomparable(element, dest)
	}
}

// Decode はエンコードされたバイト列を複数のバイト列にデコードする
//
// src: エンコードされたバイト列
//
// elements: デコード結果の格納先のポインタ
func Decode(src []byte, elements *[][]byte) {
	rest := src
	for len(rest) > 0 {
		element := make([]byte, 0)
		decodeFromMemcomparable(&rest, &element)
		*elements = append(*elements, element)
	}
}
