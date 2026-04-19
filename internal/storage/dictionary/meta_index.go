package dictionary

import (
	"encoding/binary"
	"minesql/internal/storage/btree"
	"minesql/internal/storage/btree/node"
	"minesql/internal/storage/buffer"
	"minesql/internal/storage/encode"
	"minesql/internal/storage/page"
)

type IndexType string

const (
	IndexTypeUnique    IndexType = "unique secondary"
	IndexTypeNonUnique IndexType = "non-unique secondary"
)

// IndexMeta はセカンダリインデックスのメタデータを表す
//
// 参考: https://dev.mysql.com/doc/refman/8.0/ja/information-schema-innodb-indexes-table.html
type IndexMeta struct {
	MetaPageId     page.PageId // インデックスのメタデータが格納される B+Tree のメタページID
	FileId         page.FileId // インデックスが属するテーブルの FileId
	Name           string      // インデックス名
	ColName        string      // インデックスを構成するカラム名
	Type           IndexType   // インデックスの種類
	DataMetaPageId page.PageId // 実データが格納される B+Tree のメタページID
}

func NewIndexMeta(fileId page.FileId, name string, colName string, indexType IndexType, dataMetaPageId page.PageId) *IndexMeta {
	return &IndexMeta{
		FileId:         fileId,
		Name:           name,
		ColName:        colName,
		Type:           indexType,
		DataMetaPageId: dataMetaPageId,
	}
}

// Insert はインデックスメタデータを B+Tree に挿入する
func (im *IndexMeta) Insert(bp *buffer.BufferPool) error {
	btr := btree.NewBTree(im.MetaPageId)

	// キーフィールドをエンコード (FileId + Name)
	var encodedKey []byte
	keyBuf := binary.BigEndian.AppendUint32(nil, uint32(im.FileId))
	encode.Encode([][]byte{keyBuf, []byte(im.Name)}, &encodedKey)

	// 非キーフィールドをエンコード (ColName, Type, DataMetaPageId)
	var encodedNonKey []byte
	encode.Encode([][]byte{[]byte(im.Type), []byte(im.ColName), im.DataMetaPageId.ToBytes()}, &encodedNonKey)

	// B+Tree に挿入
	return btr.Insert(bp, node.NewRecord(nil, encodedKey, encodedNonKey))
}

// loadIndexMeta は指定されたテーブルのインデックスメタデータを読み込む
//
// インデックスメタデータの B+Tree を走査して、指定されたテーブルのインデックスを収集する
//
//   - bp: BufferPool
//   - fileId: インデックスメタデータを読み込む対象のテーブルの FileId
//   - metaPageId: インデックスメタデータが格納されている B+Tree のメタページID
func loadIndexMeta(bp *buffer.BufferPool, fileId page.FileId, metaPageId page.PageId) ([]*IndexMeta, error) {
	// B+Tree を開く
	idxMetaTree := btree.NewBTree(metaPageId)
	iter, err := idxMetaTree.Search(bp, btree.SearchModeStart{})
	if err != nil {
		return nil, err
	}

	var indexes []*IndexMeta
	for {
		record, ok := iter.Get()
		if !ok {
			break
		}

		// キーフィールドをデコード (FileId, Name)
		var keyParts [][]byte
		encode.Decode(record.KeyBytes(), &keyParts)
		idxFileId := page.FileId(binary.BigEndian.Uint32(keyParts[0]))

		// 指定されたテーブルのインデックスのみを収集
		if idxFileId == fileId {
			idxName := string(keyParts[1])

			// 非キーフィールドをデコード (Type, ColName, DataMetaPageId)
			var nonKeyParts [][]byte
			encode.Decode(record.NonKeyBytes(), &nonKeyParts)
			idxType := IndexType(string(nonKeyParts[0]))
			colName := string(nonKeyParts[1])
			dataMetaPageId := page.RestorePageIdFromBytes(nonKeyParts[2])

			indexes = append(indexes, &IndexMeta{
				FileId:         fileId,
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
