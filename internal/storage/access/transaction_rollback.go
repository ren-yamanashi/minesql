package access

import (
	"bytes"
	"errors"

	"github.com/ren-yamanashi/minesql/internal/storage/btree"
	"github.com/ren-yamanashi/minesql/internal/storage/btree/node"
	"github.com/ren-yamanashi/minesql/internal/storage/encode"
	"github.com/ren-yamanashi/minesql/internal/storage/page"
	"github.com/ren-yamanashi/minesql/internal/storage/undo"
)

// rollbackRecord は 1 つの Undo レコードに対応するロールバック操作を実行する
func (t *TrxManager) rollbackRecord(record undo.Record) error {
	fileId := record.TableFileId()
	piRecord, err := fetchPrimaryIndexRecord(t.catalog, fileId)
	if err != nil {
		return err
	}
	primaryTree := btree.NewBtree(t.bufferPool, piRecord.MetaPageId)

	switch r := record.(type) {
	case undo.InsertRecord:
		return t.rollbackInsert(primaryTree, r, fileId)
	case undo.DeleteRecord:
		return t.rollbackDelete(primaryTree, r, fileId)
	case undo.UpdateRecord:
		return t.rollbackUpdate(primaryTree, r, fileId)
	default:
		return errors.New("unknown undo record type")
	}
}

// rollbackInsert は Insert を取り消す (Primary, Secondary の物理削除)
func (t *TrxManager) rollbackInsert(primaryTree *btree.Btree, record undo.InsertRecord, fileId page.FileId) error {
	if err := primaryTree.Delete(record.Record.Key()); err != nil {
		return err
	}
	primaryRecord, err := decodePrimaryRecord(record.Record, t.catalog, fileId)
	if err != nil {
		return err
	}
	return t.forEachSecondaryTree(fileId, func(tree *btree.Btree, keyCols map[string]int) error {
		key := encodeSecondaryKey(primaryRecord, keyCols)
		return tree.Delete(key)
	})
}

// rollbackDelete は SoftDelete を取り消す (Primary, Secondary の DeleteMark 復元)
func (t *TrxManager) rollbackDelete(primaryTree *btree.Btree, record undo.DeleteRecord, fileId page.FileId) error {
	// Undo ログには削除前のレコード (DeleteMark=0) が保存されているので、削除前のレコードで上書き
	if err := primaryTree.Update(record.Record); err != nil {
		return err
	}
	primaryRecord, err := decodePrimaryRecord(record.Record, t.catalog, fileId)
	if err != nil {
		return err
	}
	return t.forEachSecondaryTree(fileId, func(tree *btree.Btree, keyCols map[string]int) error {
		key := encodeSecondaryKey(primaryRecord, keyCols)
		restored := node.NewRecord([]byte{0}, key, nil) // header: deleteMark(0), key: sk+pk, nonKey: nil
		return tree.Update(restored)
	})
}

// rollbackUpdate は Update を取り消す (Primary を旧レコードで上書き + Secondary 復元)
func (t *TrxManager) rollbackUpdate(primaryTree *btree.Btree, record undo.UpdateRecord, fileId page.FileId) error {
	if err := primaryTree.Update(record.PrevRecord); err != nil {
		return err
	}
	prevPrimaryRecord, err := decodePrimaryRecord(record.PrevRecord, t.catalog, fileId)
	if err != nil {
		return err
	}
	newPrimaryRecord, err := decodePrimaryRecord(record.NewRecord, t.catalog, fileId)
	if err != nil {
		return err
	}
	return t.forEachSecondaryTree(fileId, func(tree *btree.Btree, keyCols map[string]int) error {
		oldKey := encodeSecondaryKey(prevPrimaryRecord, keyCols)
		newKey := encodeSecondaryKey(newPrimaryRecord, keyCols)
		// SK が変わってない場合はスキップ
		if bytes.Equal(oldKey, newKey) {
			return nil
		}
		// 更新後の SK を物理削除し、更新前の SK を復元 (論理削除を元に戻す)
		if err := tree.Delete(newKey); err != nil {
			return err
		}
		restored := node.NewRecord([]byte{0}, oldKey, nil) // header: deleteMark(0), key: sk+pk, nonKey: nil
		return tree.Update(restored)
	})
}

// forEachSecondaryTree は指定テーブルの全セカンダリインデックスに対してコールバックを実行する
func (t *TrxManager) forEachSecondaryTree(
	fileId page.FileId,
	op func(tree *btree.Btree, keyCols map[string]int) error,
) error {
	records, err := fetchSecondaryIndexRecords(t.catalog, fileId)
	if err != nil {
		return err
	}
	for _, record := range records {
		keyCols, err := fetchIndexKeyCol(t.catalog, record.IndexId)
		if err != nil {
			return err
		}
		tree := btree.NewBtree(t.bufferPool, record.MetaPageId)
		if err := op(tree, keyCols); err != nil {
			return err
		}
	}
	return nil
}

// encodeSecondaryKey は PrimaryRecord からセカンダリインデックスの B+Tree キー (SK+PK) を構築する
func encodeSecondaryKey(record *PrimaryRecord, keyCols map[string]int) []byte {
	valMap := make(map[string]string, len(record.ColNames))
	for i, name := range record.ColNames {
		valMap[name] = record.Values[i]
	}

	// SK をインデックス定義順に取得
	skValues := make([]string, len(keyCols))
	for name, pos := range keyCols {
		skValues[pos] = valMap[name]
	}

	// PK を取得
	pkValues := record.Values[:record.pkCount]

	// SK + PK をエンコード
	var key []byte
	encode.Encode(stringToByteSlice(skValues), &key)
	encode.Encode(stringToByteSlice(pkValues), &key)
	return key
}
