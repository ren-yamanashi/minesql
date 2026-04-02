package node

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNewRecord(t *testing.T) {
	t.Run("3 領域を持つレコードが作成される", func(t *testing.T) {
		// WHEN
		record := NewRecord([]byte{0}, []byte("key"), []byte("value"))

		// THEN
		assert.Equal(t, []byte{0}, record.HeaderBytes())
		assert.Equal(t, []byte("key"), record.KeyBytes())
		assert.Equal(t, []byte("value"), record.NonKeyBytes())
	})

	t.Run("nil を指定した場合でもレコードが作成される", func(t *testing.T) {
		// WHEN
		record := NewRecord(nil, []byte("key"), nil)

		// THEN
		assert.Nil(t, record.HeaderBytes())
		assert.Equal(t, []byte("key"), record.KeyBytes())
		assert.Nil(t, record.NonKeyBytes())
	})
}

func TestHeaderBytes(t *testing.T) {
	t.Run("ヘッダー領域を返す", func(t *testing.T) {
		// GIVEN
		record := NewRecord([]byte{1, 2, 3}, []byte("key"), []byte("value"))

		// THEN
		assert.Equal(t, []byte{1, 2, 3}, record.HeaderBytes())
	})
}

func TestKeyBytes(t *testing.T) {
	t.Run("キーフィールド領域を返す", func(t *testing.T) {
		// GIVEN
		record := NewRecord([]byte{0}, []byte("mykey"), []byte("myvalue"))

		// THEN
		assert.Equal(t, []byte("mykey"), record.KeyBytes())
	})
}

func TestNonKeyBytes(t *testing.T) {
	t.Run("非キーフィールド領域を返す", func(t *testing.T) {
		// GIVEN
		record := NewRecord([]byte{0}, []byte("key"), []byte("nonkey"))

		// THEN
		assert.Equal(t, []byte("nonkey"), record.NonKeyBytes())
	})

	t.Run("非キーフィールドが nil の場合 nil を返す", func(t *testing.T) {
		// GIVEN
		record := NewRecord([]byte{0}, []byte("key"), nil)

		// THEN
		assert.Nil(t, record.NonKeyBytes())
	})
}

func TestCompareKey(t *testing.T) {
	t.Run("キーが一致する場合 0 を返す", func(t *testing.T) {
		record := NewRecord(nil, []byte("bbb"), nil)
		assert.Equal(t, 0, record.CompareKey([]byte("bbb")))
	})

	t.Run("レコードのキーが小さい場合 -1 を返す", func(t *testing.T) {
		record := NewRecord(nil, []byte("aaa"), nil)
		assert.Equal(t, -1, record.CompareKey([]byte("bbb")))
	})

	t.Run("レコードのキーが大きい場合 1 を返す", func(t *testing.T) {
		record := NewRecord(nil, []byte("ccc"), nil)
		assert.Equal(t, 1, record.CompareKey([]byte("bbb")))
	})
}

func TestToBytes(t *testing.T) {
	t.Run("全フィールドが存在するレコードをシリアライズ・デシリアライズできる", func(t *testing.T) {
		// GIVEN
		original := NewRecord([]byte{1}, []byte("mykey"), []byte("myvalue"))

		// WHEN
		data := original.ToBytes()
		restored := recordFromBytes(data)

		// THEN
		assert.Equal(t, original.HeaderBytes(), restored.HeaderBytes())
		assert.Equal(t, original.KeyBytes(), restored.KeyBytes())
		assert.Equal(t, original.NonKeyBytes(), restored.NonKeyBytes())
	})

	t.Run("ヘッダーが nil のレコードをシリアライズ・デシリアライズできる", func(t *testing.T) {
		// GIVEN
		original := NewRecord(nil, []byte("key"), []byte("value"))

		// WHEN
		data := original.ToBytes()
		restored := recordFromBytes(data)

		// THEN
		assert.Equal(t, 0, len(restored.HeaderBytes()))
		assert.Equal(t, []byte("key"), restored.KeyBytes())
		assert.Equal(t, []byte("value"), restored.NonKeyBytes())
	})

	t.Run("NonKey が nil のレコードをシリアライズ・デシリアライズできる", func(t *testing.T) {
		// GIVEN
		original := NewRecord([]byte{0}, []byte("key"), nil)

		// WHEN
		data := original.ToBytes()
		restored := recordFromBytes(data)

		// THEN
		assert.Equal(t, []byte{0}, restored.HeaderBytes())
		assert.Equal(t, []byte("key"), restored.KeyBytes())
		assert.Nil(t, restored.NonKeyBytes())
	})

	t.Run("全フィールドが nil のレコードをシリアライズ・デシリアライズできる", func(t *testing.T) {
		// GIVEN
		original := NewRecord(nil, nil, nil)

		// WHEN
		data := original.ToBytes()
		restored := recordFromBytes(data)

		// THEN
		assert.Equal(t, 0, len(restored.HeaderBytes()))
		assert.Equal(t, 0, len(restored.KeyBytes()))
		assert.Nil(t, restored.NonKeyBytes())
	})

	t.Run("複数バイトのヘッダーを持つレコードをシリアライズ・デシリアライズできる", func(t *testing.T) {
		// GIVEN
		header := []byte{0, 1, 2, 3}
		original := NewRecord(header, []byte("key"), []byte("val"))

		// WHEN
		data := original.ToBytes()
		restored := recordFromBytes(data)

		// THEN
		assert.Equal(t, header, restored.HeaderBytes())
		assert.Equal(t, []byte("key"), restored.KeyBytes())
		assert.Equal(t, []byte("val"), restored.NonKeyBytes())
	})

	t.Run("ToBytes の出力サイズが 4 + header + key + nonKey と一致する", func(t *testing.T) {
		header := []byte{1, 2}
		key := []byte("hello")
		nonKey := []byte("world")
		record := NewRecord(header, key, nonKey)

		data := record.ToBytes()

		expectedSize := 4 + len(header) + len(key) + len(nonKey)
		assert.Equal(t, expectedSize, len(data))
	})
}

func TestRecordFromBytes(t *testing.T) {
	t.Run("4 バイト未満のデータは nil レコードを返す", func(t *testing.T) {
		// WHEN
		restored := recordFromBytes([]byte{0, 1})

		// THEN
		assert.Nil(t, restored.HeaderBytes())
		assert.Nil(t, restored.KeyBytes())
		assert.Nil(t, restored.NonKeyBytes())
	})

	t.Run("サイズフィールドがデータ長を超える場合 nil レコードを返す", func(t *testing.T) {
		// GIVEN: headerSize=100, keySize=100 だがデータは 4 バイトしかない
		data := []byte{0, 100, 0, 100}

		// WHEN
		restored := recordFromBytes(data)

		// THEN
		assert.Nil(t, restored.HeaderBytes())
		assert.Nil(t, restored.KeyBytes())
		assert.Nil(t, restored.NonKeyBytes())
	})
}
