package table

// [8 バイトのデータ][1 バイトの長さ情報]
const BLOCK_SIZE = 9

// memcomparable のデータ部分のサイズ
// memcomparable のデータから長さ情報を取得する際には (*src)[DATA_SIZE] のように index アクセスする
const DATA_SIZE = BLOCK_SIZE - 1

// エンコード後のサイズを計測する
// size: エンコード前のバイト列のサイズ
// 計測方法: size を 8 バイトずつに分割し、各ブロックに対して 9 バイトを割り当てる
func EncodedSize(size int) int {
	return ((size + DATA_SIZE - 1) / DATA_SIZE) * BLOCK_SIZE
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

		// データが 8 バイト未満の場合、パディングを追加
		padSize := DATA_SIZE - copySize
		if padSize > 0 {
			padding := make([]byte, padSize)
			*destination = append(*destination, padding...)
		}

		// 長さ情報を追加
		*destination = append(*destination, byte(copySize))
	}
}

// memcomparable 形式からバイト列をデコードする
// src: デコード対象のバイト列のポインタ
// destination: デコード結果の格納先のポインタ
func DecodeFromMemcomparable(src *[]byte, destination *[]byte) {
	// 次のブロック (9 バイト) が存在する限りループ
	for len(*src) >= BLOCK_SIZE {
		// 長さ情報を取得 (実データのバイト数)
		dataSize := (*src)[DATA_SIZE]

		// データをコピー
		*destination = append(*destination, (*src)[0:dataSize]...)
		*src = (*src)[BLOCK_SIZE:] // 1 ブロック (9 バイト) を削除
	}
}
