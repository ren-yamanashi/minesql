package table

// [8 バイトのデータ][1 バイトの長さ情報]
const ESCAPE_SIZE = 9
const DATA_SIZE = ESCAPE_SIZE - 1

// エンコード後のサイズを計測する
func EncodedSize(size int) int {
	return (size + (DATA_SIZE)) / (DATA_SIZE) * ESCAPE_SIZE
}

// バイト列を memcomparable 形式にエンコードする
// エンコード形式: [8 バイトのデータ][1 バイトの長さ情報] のブロックを繰り返す
// src: エンコード対象のバイト列
// destination: エンコード結果の格納先のポインタ
func EncodeToMemcomparable(src []byte, destination *[]byte) {
	for len(src) > 0 {
		// コピーサイズを決定 (最大 8 バイト, src が 8 バイト未満の場合はその長さ)
		copySize := DATA_SIZE
		if len(src) < copySize {
			copySize = len(src)
		}

		// データをコピー
		*destination = append(*destination, src[0:copySize]...)
		src = src[copySize:] // コピーした分を src から削除

		// 長さ情報を追加
		// 残りのデータがない場合は、コピーサイズ + 1 を格納
		if len(src) == 0 {
			padSize := DATA_SIZE - copySize
			// データが 8 バイト未満の場合はパディングを追加 (0 埋め)
			if padSize > 0 {
				padding := make([]byte, padSize)
				*destination = append(*destination, padding...)
			}
			*destination = append(*destination, byte(copySize+1))
			break
		}
		// 残りのデータがある場合は、固定の長さ情報を格納
		*destination = append(*destination, byte(DATA_SIZE))
	}
}

// memcomparable 形式からバイト列をデコードする
// src: デコード対象のバイト列のポインタ
// destination: デコード結果の格納先のポインタ
func DecodeFromMemcomparable(src *[]byte, destination *[]byte) {
	for {
		// コピーサイズを決定 (最大 8 バイト, src が 8 バイト未満の場合はその長さ)
		extra := (*src)[DATA_SIZE]
		size := DATA_SIZE
		if int(extra) < size {
			size = int(extra)
		}

		// データをコピー
		*destination = append(*destination, (*src)[0:size]...)
		*src = (*src)[ESCAPE_SIZE:] // コピーした分を src から削除
		if extra < byte(ESCAPE_SIZE) {
			break
		}
	}
}
