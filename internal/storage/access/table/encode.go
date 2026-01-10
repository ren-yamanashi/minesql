package table

// 複数のバイト列を連結してエンコードする
// elements: エンコード対象のバイト列のスライス
// destination: エンコード結果の格納先のポインタ
func Encode(elements [][]byte, destination *[]byte) {
	for _, element := range elements {
		size := EncodedSize(len(element))

		// destination の容量が、必要なサイズを満たしていない場合は拡張
		if cap(*destination)-len(*destination) < size {
			newData := make([]byte, len(*destination), len(*destination)+size)
			copy(newData, *destination)
			*destination = newData
		}

		// エンコード
		EncodeToMemcomparable(element, destination)
	}
}

// エンコードされたバイト列を複数のバイト列にデコードする
// src: エンコードされたバイト列
// elements: デコード結果の格納先のポインタ
func Decode(src []byte, elements *[][]byte) {
	rest := src
	for len(rest) > 0 {
		element := make([]byte, 0)
		DecodeFromMemcomparable(&rest, &element)
		*elements = append(*elements, element)
	}
}
