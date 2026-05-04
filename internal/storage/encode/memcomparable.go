package encode

const (
	lengthInfoSize = 1
	dataSize       = 8
	blockSize      = dataSize + lengthInfoSize
)

// Encode は複数のバイト列を連結してエンコードする
//   - elements: エンコード対象のバイト列のスライス
//   - dest: エンコードの結果の格納先のポインタ
func Encode(elements [][]byte, dest *[]byte) {
	for _, element := range elements {
		size := encodedSize(len(element))

		// dest の容量が必要なサイズを満たしていない場合は拡張
		if cap(*dest)-len(*dest) < size {
			newData := make([]byte, len(*dest), len(*dest)+size)
			copy(newData, *dest)
			*dest = newData
		}

		encodeToMemcomparable(element, dest)
	}
}

// Decode はエンコードされたバイト列を複数のバイト列にデコードする
//   - src: エンコードされたバイト列
//   - elements: デコード結果の格納先のポインタ
func Decode(src []byte, elements *[][]byte) {
	rest := src
	for len(rest) > 0 {
		element := []byte{}
		decodeFromMemcomparable(&rest, &element)
		*elements = append(*elements, element)
	}
}

// encodedSize はエンコード後のサイズを計測する
//   - size: エンコード前のバイト列のサイズ
//   - 計測方法: size を 8 バイトずつに分割し、各ブロックに対して 9 バイトを割り当てる
func encodedSize(size int) int {
	return ((size + dataSize - 1) / dataSize) * blockSize
}

// バイト列を memcomparable 形式にエンコードする
//   - src: エンコード対象のバイト列
//   - dest: エンコード結果の格納先ポインタ
func encodeToMemcomparable(src []byte, dest *[]byte) {
	// 8 バイトずつブロックに分割して書き込む (長さ情報には 9 を付与)
	for len(src) > dataSize {
		*dest = append(*dest, src[:dataSize]...)
		src = src[dataSize:]
		*dest = append(*dest, byte(blockSize))
	}

	// 最後のブロックを書き込む (8 バイトに満たない場合はゼロ埋め)
	copySize := len(src)
	*dest = append(*dest, src...)
	if pad := dataSize - copySize; pad > 0 {
		*dest = append(*dest, make([]byte, pad)...)
	}
	*dest = append(*dest, byte(copySize))
}

// memcomparable 形式からバイト列をデコードする
//   - src: デコード対象のバイト列のポインタ
//   - dest: デコード結果の格納先のポインタ
func decodeFromMemcomparable(src *[]byte, dest *[]byte) {
	for {
		// 長さ情報を取得
		extra := (*src)[dataSize]
		size := min(dataSize, int(extra))

		// データをコピー
		*dest = append(*dest, (*src)[0:size]...)
		*src = (*src)[blockSize:]

		// 長さ情報が 9 未満の場合、最後のブロックなので終了
		if extra < byte(blockSize) {
			break
		}
	}
}
