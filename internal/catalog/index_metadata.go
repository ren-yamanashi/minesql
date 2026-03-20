package catalog

import (
	"encoding/binary"
	"minesql/internal/storage/btree"
	"minesql/internal/storage/bufferpool"
	"minesql/internal/storage/memcomparable"
	"minesql/internal/storage/page"
)

type IndexType string

const (
	IndexTypeUnique IndexType = "unique secondary"
)

// IndexMetadata はセカンダリインデックスのメタデータを表す
//
// 参考: https://dev.mysql.com/doc/refman/8.0/ja/information-schema-innodb-indexes-table.html
type IndexMetadata struct {
	MetaPageId     page.PageId // インデックスのメタデータが格納される B+Tree のメタページID
	TableId        uint64      // インデックスが関連付けられたテーブルの識別子
	Name           string      // インデックスの名前
	ColName        string      // インデックスを構成するカラム名
	Type           IndexType   // インデックスの種類
	DataMetaPageId page.PageId // 実データが格納される B+Tree のメタページID
}

func NewIndexMetadata(tableId uint64, name string, colName string, indexType IndexType, dataMetaPageId page.PageId) *IndexMetadata {
	return &IndexMetadata{
		TableId:        tableId,
		Name:           name,
		ColName:        colName,
		Type:           indexType,
		DataMetaPageId: dataMetaPageId,
	}
}

// Insert はインデックスメタデータを B+Tree に挿入する
func (im *IndexMetadata) Insert(bp *bufferpool.BufferPool) error {
	btr := btree.NewBPlusTree(im.MetaPageId)

	// key (TableId + Name) をエンコード
	var encodedKey []byte
	keyBuf := binary.BigEndian.AppendUint64(nil, im.TableId)
	memcomparable.Encode([][]byte{keyBuf, []byte(im.Name)}, &encodedKey)

	// value (ColName, Type, DataMetaPageId) をエンコード
	var encodedValue []byte
	memcomparable.Encode([][]byte{[]byte(im.Type), []byte(im.ColName), im.DataMetaPageId.ToBytes()}, &encodedValue)

	// B+Tree に挿入
	return btr.Insert(bp, btree.NewPair(encodedKey, encodedValue))
}

// loadIndexMetadata は指定されたテーブルのインデックスメタデータを読み込む
//
// インデックスメタデータの B+Tree を走査して、指定されたテーブルのインデックスを収集する
//
// bp: BufferPool
//
// tableId: テーブルの識別子
//
// metaPageId: インデックスメタデータが格納されている B+Tree のメタページID
func loadIndexMetadata(bp *bufferpool.BufferPool, tableId uint64, metaPageId page.PageId) ([]*IndexMetadata, error) {
	// B+Tree を開く
	idxMetaTree := btree.NewBPlusTree(metaPageId)
	iter, err := idxMetaTree.Search(bp, btree.SearchModeStart{})
	if err != nil {
		return nil, err
	}

	var indexes []*IndexMetadata
	for {
		pair, ok := iter.Get()
		if !ok {
			break
		}

		// キーをデコード (TableId, Name)
		var keyParts [][]byte
		memcomparable.Decode(pair.Key, &keyParts)
		idxTableId := binary.BigEndian.Uint64(keyParts[0])

		// 指定されたテーブルのインデックスのみを収集
		if idxTableId == tableId {
			idxName := string(keyParts[1])

			// 値をデコード (Type, ColName, DataMetaPageId)
			var valueParts [][]byte
			memcomparable.Decode(pair.Value, &valueParts)
			idxType := IndexType(string(valueParts[0]))
			colName := string(valueParts[1])
			dataMetaPageId := page.RestorePageIdFromBytes(valueParts[2])

			indexes = append(indexes, &IndexMetadata{
				TableId:        tableId,
				Name:           idxName,
				ColName:        colName,
				Type:           idxType,
				DataMetaPageId: dataMetaPageId,
			})
		}

		if err := iter.Advance(bp); err != nil {
			return nil, err
		}
	}

	return indexes, nil
}
