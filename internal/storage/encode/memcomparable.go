package encode

import (
	"errors"
	"slices"
)

const (
	lengthInfoSize = 1
	dataSize       = 8
	blockSize      = dataSize + lengthInfoSize

	// continuationMarker は後続ブロックが続くことを示す長さバイトの値
	// blockSize = dataSize + lengthInfoSize なので、末尾ブロックの長さ 0..dataSize と区別できる
	continuationMarker = byte(blockSize)
)

var ErrInvalidEncoding = errors.New("encode: invalid memcomparable encoding")

// Encode は複数のバイト列を連結してエンコードする
//   - dst: 既存バイト列の末尾に追記する場合に渡す (nil 可)
//   - elements: エンコード対象のバイト列のスライス
func Encode(dst []byte, elements [][]byte) []byte {
	for _, element := range elements {
		dst = slices.Grow(dst, encodedSize(len(element)))
		dst = encodeToMemcomparable(dst, element)
	}
	return dst
}

// Decode はエンコードされたバイト列を複数のバイト列にデコードする
func Decode(src []byte) ([][]byte, error) {
	var elements [][]byte
	rest := src
	for len(rest) > 0 {
		element, consumed, err := decodeFromMemcomparable(rest)
		if err != nil {
			return nil, err
		}
		elements = append(elements, element)
		rest = rest[consumed:]
	}
	return elements, nil
}

// encodedSize はエンコード後のサイズを計測する
//   - size: エンコード前のバイト列のサイズ
//   - 計測方法: size を 8 バイトずつに分割し、各ブロックに対して 9 バイトを割り当てる
func encodedSize(size int) int {
	if size == 0 {
		return blockSize
	}
	return ((size + dataSize - 1) / dataSize) * blockSize
}

// encodeToMemcomparable は単一のバイト列を memcomparable 形式にエンコードし、dst の末尾に追記して返す
func encodeToMemcomparable(dst, src []byte) []byte {
	// 8 バイトずつブロックに分割して書き込む (長さ情報には continuationMarker を付与)
	for len(src) > dataSize {
		dst = append(dst, src[:dataSize]...)
		src = src[dataSize:]
		dst = append(dst, continuationMarker)
	}

	// 最後のブロックを書き込む (8 バイトに満たない場合はゼロ埋め)
	copySize := len(src)
	dst = append(dst, src...)
	if pad := dataSize - copySize; pad > 0 {
		dst = append(dst, make([]byte, pad)...)
	}
	dst = append(dst, byte(copySize))
	return dst
}

// decodeFromMemcomparable は src の先頭から 1 要素分を memcomparable 形式からデコードし、デコード結果と消費したバイト数を返す
func decodeFromMemcomparable(src []byte) ([]byte, int, error) {
	dst := []byte{}
	consumed := 0
	for {
		if len(src) < blockSize {
			return nil, 0, ErrInvalidEncoding
		}

		// 長さ情報を取得
		lengthByte := src[dataSize]
		if lengthByte > continuationMarker {
			return nil, 0, ErrInvalidEncoding
		}

		// データをコピー
		size := min(dataSize, int(lengthByte))
		dst = append(dst, src[:size]...)
		src = src[blockSize:]
		consumed += blockSize

		// 長さ情報が continuationMarker 未満の場合、最後のブロックなので終了
		if lengthByte < continuationMarker {
			return dst, consumed, nil
		}
	}
}
