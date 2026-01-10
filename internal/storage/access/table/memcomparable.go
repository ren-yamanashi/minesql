package table

// [8 バイトのデータ][1 バイトの長さ情報]
// 長さ情報: 次のブロックがある場合は 9、最後のブロックは実データ長 (0-8)
const BLOCK_SIZE = 9

// memcomparable のデータ部分のサイズ
// memcomparable のデータから長さ情報を取得する際には (*src)[DATA_SIZE] のように index アクセスする
const DATA_SIZE = BLOCK_SIZE - 1

// エンコード後のサイズを計測する
// size: エンコード前のバイト列のサイズ
// 計測方法: size を 8 バイトずつに分割し、各ブロックに対して 9 バイトを割り当てる
func encodedSize(size int) int {
	return ((size + DATA_SIZE - 1) / DATA_SIZE) * BLOCK_SIZE
}

// バイト列を memcomparable 形式にエンコードする
// エンコード形式: [8 バイトのデータ][1 バイトの長さ情報] のブロックを繰り返す
// src: エンコード対象のバイト列
// destination: エンコード結果の格納先のポインタ
func encodeToMemcomparable(src []byte, destination *[]byte) {
	for len(src) > 0 {
		// コピーサイズを決定 (最大 8 バイト, src が 8 バイト未満の場合はその長さ)
		copySize := DATA_SIZE
		if len(src) < copySize {
			copySize = len(src)
		}

		// データをコピー
		*destination = append(*destination, src[0:copySize]...)
		src = src[copySize:] // コピーした分を src から削除

		// src がまだ残っている場合、次のブロックがあることを示す 9 を追加
		if len(src) > 0 {
			*destination = append(*destination, byte(BLOCK_SIZE))
			continue
		}

		// src が空の場合、最後のブロックとして処理 (データが 8 バイト未満の場合、パディングを追加)
		padSize := DATA_SIZE - copySize
		if padSize > 0 {
			padding := make([]byte, padSize)
			*destination = append(*destination, padding...)
		}
		*destination = append(*destination, byte(copySize))
		break
	}
}

// memcomparable 形式からバイト列をデコードする
// src: デコード対象のバイト列のポインタ
// destination: デコード結果の格納先のポインタ
func decodeFromMemcomparable(src *[]byte, destination *[]byte) {
	for {
		// 長さ情報を取得
		extra := (*src)[DATA_SIZE]
		size := DATA_SIZE
		if int(extra) < size {
			size = int(extra)
		}

		// データをコピー
		*destination = append(*destination, (*src)[0:size]...)
		*src = (*src)[BLOCK_SIZE:]

		// 長さ情報が 9 未満の場合、最後のブロックなので終了
		if extra < byte(BLOCK_SIZE) {
			break
		}
	}
}
