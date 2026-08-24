package access

import (
	"github.com/ren-yamanashi/minesql/internal/storage/btree"
	"github.com/ren-yamanashi/minesql/internal/storage/dictionary"
	"github.com/ren-yamanashi/minesql/internal/storage/page"
	"github.com/ren-yamanashi/minesql/internal/storage/undo"
)

// createSecondaryIndexes はセカンダリインデックスを作成する
//   - 各セカンダリインデックスごとに 1 mtr で「B+Tree 作成 + CreateBTreeUndo + MetaInsertUndo + IndexMeta Insert + 各 MetaInsertUndo + IndexKeyColumnMeta Insert」を原子的に記録する
//   - 途中失敗時もその mtr は commit される (既に書き込まれた分は DDL Undo 経由で取り消す)
func createSecondaryIndexes(
	ddlTrx *Transaction,
	fileId page.FileId,
	pt *btree.Tree,
	inputs []CreateIndexInput,
) ([]*secondaryIndex, error) {
	indexes := make([]*secondaryIndex, 0, len(inputs))
	for _, input := range inputs {
		index, err := buildOneSecondaryIndex(ddlTrx, fileId, pt, input)
		if err != nil {
			return nil, err
		}
		indexes = append(indexes, index)
	}
	return indexes, nil
}

// buildOneSecondaryIndex はセカンダリインデックス 1 件を 1 mtr で作成する
//   - B+Tree 作成、 CreateBTreeUndo、 MetaInsertUndo + IndexMeta Insert、 各 MetaInsertUndo + IndexKeyColumnMeta Insert を全て同一 mtr で行う
//   - エラー時も mtr は commit される
func buildOneSecondaryIndex(
	ddlTrx *Transaction,
	fileId page.FileId,
	pt *btree.Tree,
	input CreateIndexInput,
) (index *secondaryIndex, retErr error) {
	ct := ddlTrx.catalog
	bp := ddlTrx.bufferPool
	mtr := ddlTrx.NewMtr()
	defer func() {
		if commitErr := mtr.Commit(); commitErr != nil && retErr == nil {
			retErr = commitErr
		}
	}()

	indexId, err := ct.AllocateIndexId(mtr)
	if err != nil {
		return nil, err
	}

	index, err = createSecondaryIndex(mtr, ct, bp, createSecondaryIndexInput{
		FileId:      fileId,
		PrimaryTree: pt,
		IndexId:     indexId,
		IndexName:   input.IndexName,
		Unique:      input.IndexType == dictionary.IndexTypeUnique,
		Lock:        ddlTrx.lockMgr,
		UndoLog:     ddlTrx.undoLog,
	})
	if err != nil {
		return nil, err
	}

	createBTreeUndo := undo.NewDDLRecord(
		undo.DDLRecordTypeCreateBTree,
		undo.NewCreateBTreeUndoRecord(index.tree.MetaPageId()).Serialize(),
	)
	if err := ddlTrx.DDLManager().Append(mtr, createBTreeUndo); err != nil {
		return nil, err
	}

	indexRecord := dictionary.NewIndexMetaRecord(
		fileId,
		indexId,
		input.IndexName,
		input.IndexType,
		len(input.ColNames),
		index.tree.MetaPageId(),
	)
	indexKey := indexRecord.Encode().Key()
	if err := appendMetaInsertUndo(ddlTrx, mtr, undo.MetaTableTypeIndex, indexKey); err != nil {
		return nil, err
	}
	if err := ct.IndexMeta().Insert(mtr, indexRecord); err != nil {
		return nil, err
	}

	for i, keyCol := range input.ColNames {
		keyColRecord := dictionary.NewIndexKeyColumnMetaRecord(indexId, keyCol, i)
		keyColKey := keyColRecord.Encode().Key()
		if err := appendMetaInsertUndo(ddlTrx, mtr, undo.MetaTableTypeIndexKeyColumn, keyColKey); err != nil {
			return nil, err
		}
		if err := ct.IndexKeyColumnMeta().Insert(mtr, keyColRecord); err != nil {
			return nil, err
		}
	}
	return index, nil
}
