package node

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNewRecord(t *testing.T) {
	t.Run("Header, Key, NonKey を取得できる", func(t *testing.T) {
		// GIVEN
		header := []byte{0x01}
		key := []byte{0x02, 0x03}
		nonKey := []byte{0x04, 0x05, 0x06}

		// WHEN
		r := NewRecord(header, key, nonKey)

		// THEN
		assert.Equal(t, header, r.Header())
		assert.Equal(t, key, r.Key())
		assert.Equal(t, nonKey, r.NonKey())
	})
}

func TestCompareKey(t *testing.T) {
	t.Run("キーが一致する場合は 0 を返す", func(t *testing.T) {
		// GIVEN
		r := NewRecord([]byte{0x00}, []byte{0x01, 0x02}, []byte{})

		// WHEN
		result := r.CompareKey([]byte{0x01, 0x02})

		// THEN
		assert.Equal(t, 0, result)
	})

	t.Run("キーが小さい場合は -1 を返す", func(t *testing.T) {
		// GIVEN
		r := NewRecord([]byte{0x00}, []byte{0x01}, []byte{})

		// WHEN
		result := r.CompareKey([]byte{0x02})

		// THEN
		assert.Equal(t, -1, result)
	})

	t.Run("キーが大きい場合は 1 を返す", func(t *testing.T) {
		// GIVEN
		r := NewRecord([]byte{0x00}, []byte{0x02}, []byte{})

		// WHEN
		result := r.CompareKey([]byte{0x01})

		// THEN
		assert.Equal(t, 1, result)
	})
}

func TestToBytes(t *testing.T) {
	t.Run("シリアライズしたバイト列から復元できる", func(t *testing.T) {
		// GIVEN
		r := NewRecord([]byte{0x01}, []byte{0x02, 0x03}, []byte{0x04, 0x05, 0x06})

		// WHEN
		data := r.ToBytes()
		restored := recordFromBytes(data)

		// THEN
		assert.Equal(t, r.Header(), restored.Header())
		assert.Equal(t, r.Key(), restored.Key())
		assert.Equal(t, r.NonKey(), restored.NonKey())
	})

	t.Run("NonKey が空でも正しくシリアライズされる", func(t *testing.T) {
		// GIVEN
		r := NewRecord([]byte{0x01}, []byte{0x02}, []byte{})

		// WHEN
		data := r.ToBytes()
		restored := recordFromBytes(data)

		// THEN
		assert.Equal(t, r.Header(), restored.Header())
		assert.Equal(t, r.Key(), restored.Key())
	})

	t.Run("先頭 4 バイトに headerSize と keySize が格納される", func(t *testing.T) {
		// GIVEN
		header := []byte{0xAA, 0xBB}
		key := []byte{0xCC, 0xDD, 0xEE}

		// WHEN
		data := NewRecord(header, key, []byte{}).ToBytes()

		// THEN
		assert.Equal(t, byte(0), data[0])
		assert.Equal(t, byte(2), data[1]) // headerSize = 2
		assert.Equal(t, byte(0), data[2])
		assert.Equal(t, byte(3), data[3]) // keySize = 3
	})

	t.Run("データが 4 バイト未満の場合は nil レコードを返す", func(t *testing.T) {
		// GIVEN
		data := []byte{0x00, 0x01}

		// WHEN
		r := recordFromBytes(data)

		// THEN
		assert.Nil(t, r.Header())
		assert.Nil(t, r.Key())
		assert.Nil(t, r.NonKey())
	})

	t.Run("データ長がヘッダーとキーの合計より短い場合は nil レコードを返す", func(t *testing.T) {
		// GIVEN
		data := []byte{0x00, 0x02, 0x00, 0x03, 0xFF} // headerSize=2, keySize=3 だが残り 1 バイトしかない

		// WHEN
		r := recordFromBytes(data)

		// THEN
		assert.Nil(t, r.Header())
		assert.Nil(t, r.Key())
		assert.Nil(t, r.NonKey())
	})
}
